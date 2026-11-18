//! Native state capability for one executing delivery.
use acyclic_actors_guest_wire::{ActorStateCommit, ActorStateSeed};
use bytes::Bytes;

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
