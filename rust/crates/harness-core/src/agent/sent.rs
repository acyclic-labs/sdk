//! A message's original delivery receipt, selected through its sender's intent.
//! The marker and inbox effect share one publication; recovery reads that exact
//! immutable envelope, never a current inbox tail or a conversation scan.
use super::{AgentPath, AgentRecord, Delivered, Error, InboxRecord, IntentId, Sender};
use acyclic_actors::route::{self, Routed};
use acyclic_stream::{CommittedMutation, StreamError, StreamProviderExt as _};
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

impl Serialize for SentDelivery {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum View<'a> {
            Committed {
                inbox: u64,
            },
            Closed,
            Relayed {
                relay_seq: u64,
            },
            Rejected {
                reason: &'static str,
            },
            PendingAdmission {
                source: &'a acyclic_stream::StreamPath,
                from: u64,
            },
        }
        match self {
            Self::Complete(Routed::Delivered(delivered)) => View::Committed {
                inbox: delivered.inbox,
            },
            Self::Complete(Routed::Rejected(route::Rejection::Closed)) => View::Closed,
            Self::Complete(Routed::Rejected(route::Rejection::NoSuchActor)) => View::Rejected {
                reason: "no_such_actor",
            },
            Self::Complete(Routed::Relayed { relay_seq, .. }) => View::Relayed {
                relay_seq: *relay_seq,
            },
            Self::Pending { source, from } => View::PendingAdmission {
                source,
                from: *from,
            },
        }
        .serialize(serializer)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SentRecord {
    pub to: AgentPath,
    pub id: IntentId,
}

impl AgentRecord for SentRecord {}

/// The recipient and original outcome of this sender's committed message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentMessage {
    /// Exact recipient, including when its closed inbox skipped the effect.
    pub to: AgentPath,
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
    pub to: AgentPath,
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
    let actor = super::handler::Agents::<P>::actor(within, &message.to.id())?;
    let agents = super::handler::Agents::new(super::actor_store(provider)?);
    let (mut head, cursor) = agents.head(&actor, &message.to).await?;
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
    let head = match provider.head(sender.sent_message(id)?).await {
        Ok(head) => head,
        Err(error) if error.category() == StreamError::NotFound => return Ok(None),
        Err(error) => return Err(Error::Stream(error)),
    };
    let record = head
        .last
        .ok_or(Error::Record("send receipt marker is empty"))?;
    if head.tail != 1 || record.sequence != 0 {
        return Err(Error::Record("send receipt marker is not immutable"));
    }
    let marker = SentRecord::decode(&record.value)?;
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
    if marker.id != *id {
        return Err(Error::Record("send receipt intent differs"));
    }
    let envelope = provider
        .read_commit(record.commit_id)
        .await
        .map_err(Error::Stream)?;
    if envelope.commit_id != record.commit_id {
        return Err(Error::Record("send receipt publication differs"));
    }
    let inbox = marker.to.inbox()?;
    let mut delivery = Delivered::Closed;
    let mut routed = None;
    for mutation in envelope.mutations {
        let CommittedMutation::Append(append) = mutation else {
            continue;
        };
        if outbox.as_ref() == Some(&append.path) {
            let (paths, node) =
                route::Paths::from_outbox(&append.path).map_err(super::routing::error)?;
            for record in append.records {
                let route::Publication::Delivery(envelope) =
                    route::Publication::decode(&record.value).map_err(super::routing::error)?
                else {
                    continue;
                };
                if envelope.source != node {
                    return Err(Error::Record("send outbox source differs"));
                }
                let peer = super::PeerRoute::parse(&envelope)?;
                if peer.sender() != sender || peer.recipient() != &marker.to || peer.id() != id {
                    continue;
                }
                if routed.is_some() {
                    return Err(Error::Record("send outbox intent differs"));
                }
                routed = Some(
                    match route::receipt(
                        provider,
                        &paths,
                        &node,
                        record.sequence,
                        &envelope.request.idempotency_key,
                    )
                    .await
                    .map_err(super::routing::error)?
                    {
                        Some(receipt) => SentDelivery::Complete(receipt),
                        None => SentDelivery::Pending {
                            source: paths.outbox_cursor(&node).map_err(Error::Stream)?,
                            from: record.sequence,
                        },
                    },
                );
            }
            continue;
        }
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
    if outbox.is_some() && (routed.is_none() || matches!(delivery, Delivered::Committed { .. })) {
        return Err(Error::Record("send outbox publication differs"));
    }
    Ok(Some(SentMessage {
        to: marker.to,
        id: marker.id,
        delivery: routed.unwrap_or_else(|| {
            SentDelivery::Complete(match delivery {
                Delivered::Committed { inbox } => Routed::Delivered(route::Delivered { inbox }),
                Delivered::Closed => Routed::Rejected(route::Rejection::Closed),
            })
        }),
    }))
}
