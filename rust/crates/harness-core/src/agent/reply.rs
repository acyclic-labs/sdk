//! The committed reply to one accepted message: every host and frontend
//! correlates a prompt with its settled turn here, and nowhere else.
//!
//! A request is its message's [`IntentId`] and the inbox sequence its commit
//! returned (an exact replay returns the same sequence). An agent appends its
//! own conversation entries in strictly increasing inbox order: inputs are
//! queued and read back in inbox order, and an approval response taken out of
//! order resumes the waiting turn instead of starting an entry. So the entry
//! answering a request is found by binary search over the agent's own committed
//! range, with point reads through the canonical stream and no index or copy.

use super::{AgentRecord as _, AgentState, ConversationEntry, Error, IntentId, TurnInput};
use acyclic_stream::{ReadRequest, StreamPath, StreamProvider};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

/// Where one accepted message stands in the committed agent state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reply {
    /// Its turn settled as conversation entry `sequence`.
    Answered {
        /// Conversation sequence of the entry.
        sequence: u64,
        /// The settled entry; its output is read through the agent's scoped reader.
        entry: ConversationEntry,
    },
    /// Unread, queued or in the running turn.
    Pending,
    /// Consumed without a turn: a cancel or archive cleared it.
    Dropped,
}

/// The reply to message `id`, committed at inbox sequence `inbox`, in the
/// agent's committed checkpoint `state` whose step consumed its inbox before
/// `cursor`. Only `state.conversation_tail` bounds the search, so an output
/// staged by an uncommitted step is never a reply.
///
/// # Errors
/// [`Error::Conflict`] when the entry at `inbox` settled another request;
/// stream failures and malformed entries as themselves.
pub async fn reply<P: StreamProvider + ?Sized>(
    provider: &P,
    state: &AgentState,
    cursor: u64,
    id: &IntentId,
    inbox: u64,
) -> Result<Reply, Error> {
    let conversation = state.path.conversation()?;
    let (mut low, mut high) = (state.conversation_base, state.conversation_tail);
    while low < high {
        let middle = low + (high - low) / 2;
        let entry = entry(provider, &conversation, middle).await?;
        match entry.inbox.cmp(&inbox) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                return match &entry.input {
                    TurnInput::Message { id: settled, .. } if settled == id => {
                        Ok(Reply::Answered {
                            sequence: middle,
                            entry,
                        })
                    }
                    _ => Err(Error::Conflict(
                        "the inbox sequence settled another request",
                    )),
                };
            }
        }
    }
    let queued = state.queued.iter().any(|queued| queued.inbox == inbox);
    let unread = state
        .overflow
        .is_some_and(|first| inbox >= first && !state.taken.contains(&inbox));
    Ok(
        if inbox >= cursor
            || queued
            || unread
            || state.turn.as_ref().is_some_and(|turn| turn.inbox == inbox)
        {
            Reply::Pending
        } else {
            Reply::Dropped
        },
    )
}

/// Committed conversation entry `sequence`.
async fn entry<P: StreamProvider + ?Sized>(
    provider: &P,
    conversation: &StreamPath,
    sequence: u64,
) -> Result<ConversationEntry, Error> {
    let mut records = provider
        .read(ReadRequest {
            path: conversation.clone(),
            from: sequence,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .map_err(Error::Stream)?;
    match records.try_next().await.map_err(Error::Stream)? {
        Some(record) if record.sequence == sequence => ConversationEntry::decode(&record.value),
        _ => Err(Error::Record("committed conversation entry is missing")),
    }
}
