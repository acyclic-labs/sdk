//! A message's original delivery receipt, selected through its sender's intent.
//! The marker and inbox effect share one publication; recovery reads that exact
//! immutable envelope, never a current inbox tail or a conversation scan.
use super::{AgentPath, AgentRecord, Delivered, Error, InboxRecord, IntentId, Sender};
use acyclic_stream::{CommittedMutation, StreamError, StreamProviderExt as _};
use serde::{Deserialize, Serialize};

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
    /// The sequence assigned by the owning node's inbox commit, or closed.
    pub delivery: Delivered,
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
        Err(StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(Error::Stream(error)),
    };
    let record = head
        .last
        .ok_or(Error::Record("send receipt marker is empty"))?;
    if head.tail != 1 || record.sequence != 0 {
        return Err(Error::Record("send receipt marker is not immutable"));
    }
    let marker = SentRecord::decode(&record.value)?;
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
        delivery,
    }))
}
