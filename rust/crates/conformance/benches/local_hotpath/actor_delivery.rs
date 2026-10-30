//! Observe the original handler boundary without replacing its implementation.
use super::report::{Result, emit};
use acyclic_actors::{
    addressing::ActorReference,
    guest::{
        CancellationToken, Checkpoint, Delivery, GuestFuture, Handler, Manifest, Transition,
        Trigger,
    },
};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::oneshot;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum EntryError {
    ProducerDropped,
    UnexpectedDelivery,
    ConcurrentObservation,
    Poisoned,
}
impl fmt::Display for EntryError {
    fn fmt(&self, text: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(text, "handler entry: {self:?}")
    }
}
impl std::error::Error for EntryError {}

struct Pending {
    actor: ActorReference,
    sequence: u64,
    sender: oneshot::Sender<std::result::Result<Instant, EntryError>>,
}

#[derive(Default)]
pub(super) struct Entries {
    pending: Mutex<Option<Pending>>,
    calls: AtomicU64,
}

/// The delivery scope owns the producer. Completion, failure and cancellation
/// all close it even if the host never enters the handler.
pub(super) struct Scope(Arc<Entries>);
impl Drop for Scope {
    fn drop(&mut self) {
        self.0
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

impl Entries {
    pub(super) fn arm(
        self: &Arc<Self>,
        actor: ActorReference,
        sequence: u64,
    ) -> Result<(
        Scope,
        oneshot::Receiver<std::result::Result<Instant, EntryError>>,
    )> {
        let mut pending = self.pending.lock().map_err(|_| EntryError::Poisoned)?;
        if pending.is_some() {
            return Err(EntryError::ConcurrentObservation.into());
        }
        let (sender, receiver) = oneshot::channel();
        *pending = Some(Pending {
            actor,
            sequence,
            sender,
        });
        Ok((Scope(self.clone()), receiver))
    }

    pub(super) async fn receive(
        receiver: oneshot::Receiver<std::result::Result<Instant, EntryError>>,
    ) -> std::result::Result<Instant, EntryError> {
        receiver.await.map_err(|_| EntryError::ProducerDropped)?
    }

    pub(super) fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    pub(super) async fn check_dropped_producer(self: &Arc<Self>, actor: ActorReference) -> Result {
        let (scope, receiver) = self.arm(actor, 0)?;
        drop(scope);
        if Self::receive(receiver).await != Err(EntryError::ProducerDropped) {
            return Err("handler-entry control did not report its dropped producer".into());
        }
        emit(
            serde_json::json!({"schema": 1, "kind": "negative_control", "control": "dropped handler-entry delivery scope", "result": "typed producer loss"}),
        )
    }
}

pub(super) struct Observed {
    pub(super) handler: Arc<dyn Handler>,
    pub(super) entries: Arc<Entries>,
}
impl Handler for Observed {
    fn manifest(&self) -> &Manifest {
        self.handler.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        let entered = Instant::now();
        self.entries.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(pending) = self
            .entries
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let expected = pending.actor == delivery.actor
                && matches!(&delivery.trigger, Trigger::Record { sequence, .. } if *sequence == pending.sequence);
            let _ = pending.sender.send(if expected {
                Ok(entered)
            } else {
                Err(EntryError::UnexpectedDelivery)
            });
        }
        self.handler.on_record(delivery, checkpoint, cancellation)
    }
}
