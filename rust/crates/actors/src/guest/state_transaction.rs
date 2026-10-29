//! Public state-binding packets. They contain values, never authority or a path.
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// One metadata assignment in the executing actor's transaction.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ActorMetadataWrite {
    /// Actor-local metadata key.
    pub key: String,
    /// Opaque value; copied at the guest boundary.
    #[ts(type = "Uint8Array")]
    pub value: Bytes,
}
/// Original actor alarm change, applied with metadata and journal frames.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ActorStateAlarm {
    /// Keep the current generation.
    Keep,
    /// Replace with a strictly positive canonical decimal Unix-ms deadline.
    Set {
        /// Exact u64 decimal, never a floating-point timestamp.
        deadline: String,
    },
    /// Clear and invalidate the current generation.
    Clear,
}
/// Host-loaded original state, retained for this binding's admitted cut.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ActorStateSeed {
    /// Metadata from that same cut, without a second remote read.
    pub metadata: Vec<ActorMetadataWrite>,
    /// Exact u64 decimal committed frame count.
    pub journal_tail: String,
}
/// One publication's synchronous metadata, original journal frames and alarm.
/// The host retains its actor/epoch, exact cut and stable publication identity;
/// none can be selected or replaced by this guest packet.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ActorStateCommit {
    /// Last assignment for each changed metadata key.
    pub writes: Vec<ActorMetadataWrite>,
    /// Original committed journal tail, as exact u64 decimal.
    pub journal_from: String,
    /// Opaque canonical journal frames, appended in order.
    #[ts(type = "Array<Uint8Array>")]
    pub journal: Vec<Bytes>,
    /// Alarm mutation in this same conditional commit.
    pub alarm: ActorStateAlarm,
}
