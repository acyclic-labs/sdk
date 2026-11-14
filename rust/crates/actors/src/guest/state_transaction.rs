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
    /// Keys removed in this same publication, before applying assignments.
    pub deletions: Vec<String>,
    /// Original committed journal tail, as exact u64 decimal.
    pub journal_from: String,
    /// Opaque canonical journal frames, appended in order.
    #[ts(type = "Array<Uint8Array>")]
    pub journal: Vec<Bytes>,
    /// Alarm mutation in this same conditional commit.
    pub alarm: ActorStateAlarm,
}

/// One executing delivery's native state capability. Every name selecting
/// actor state shares this owner. The runtime retires it before publishing the
/// final transition; late calls fail without provider I/O.
#[derive(Clone, Debug)]
pub struct ActorStateHost {
    pub(crate) commands: tokio::sync::mpsc::Sender<StateCommand>,
    pub(crate) stopped: super::CancellationToken,
}

#[derive(Debug)]
pub(crate) enum StateCommand {
    Seed(tokio::sync::oneshot::Sender<Result<ActorStateSeed, super::GuestError>>),
    Commit(
        ActorStateCommit,
        tokio::sync::oneshot::Sender<Result<(), super::GuestError>>,
    ),
    Stage(
        ActorStateCommit,
        tokio::sync::oneshot::Sender<Result<(), super::GuestError>>,
    ),
    Read(
        u64,
        u32,
        tokio::sync::oneshot::Sender<Result<Vec<Bytes>, super::GuestError>>,
    ),
}

impl ActorStateHost {
    async fn request<T>(
        &self,
        command: impl FnOnce(tokio::sync::oneshot::Sender<Result<T, super::GuestError>>) -> StateCommand,
    ) -> Result<T, super::GuestError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        tokio::select! {
            biased;
            () = self.stopped.cancelled() => return Err(super::GuestError::Revoked),
            sent = self.commands.send(command(reply)) => sent.map_err(|_| super::GuestError::Revoked)?,
        }
        tokio::select! {
            biased;
            () = self.stopped.cancelled() => Err(super::GuestError::Revoked),
            reply = receive => reply.map_err(|_| super::GuestError::Revoked)?,
        }
    }

    /// Metadata and journal tail from the original admitted cut, with no read.
    pub async fn seed(&self) -> Result<ActorStateSeed, super::GuestError> {
        self.request(StateCommand::Seed).await
    }

    /// Apply and durably flush one canonical packet. Failure closes this
    /// capability. The runtime retains an unknown publication itself.
    pub async fn commit(&self, packet: ActorStateCommit) -> Result<(), super::GuestError> {
        self.request(|reply| StateCommand::Commit(packet, reply))
            .await
    }

    /// Retain the final unflushed packet for this delivery's publication.
    /// This acknowledgement is not durability: the packet, checkpoint,
    /// effects and consumed input publish together only after the handler
    /// returns successfully. Failure discards it but preserves prior flushes.
    /// No further flush or final packet is accepted after this handoff.
    pub async fn stage(&self, packet: ActorStateCommit) -> Result<(), super::GuestError> {
        self.request(|reply| StateCommand::Stage(packet, reply))
            .await
    }

    /// Read original committed journal frames, excluding pending writes.
    pub async fn read_journal(
        &self,
        from: u64,
        limit: u32,
    ) -> Result<Vec<Bytes>, super::GuestError> {
        self.request(|reply| StateCommand::Read(from, limit, reply))
            .await
    }
}
