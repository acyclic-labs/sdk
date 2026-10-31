//! Observe the original handler boundary without replacing its implementation.
use super::report::{Counters, DurabilityQuery, Result, emit};
use acyclic_actors::{
    addressing::ActorReference,
    guest::{
        CancellationToken, Checkpoint, Delivery, GuestFuture, Handler, Manifest, Transition,
        Trigger,
    },
};
use acyclic_stream::testing::RequestLedger;
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
    epoch: u64,
    trigger: Trigger,
    sender: Option<oneshot::Sender<std::result::Result<Entered, EntryError>>>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Entered {
    pub(super) at: Instant,
    pub(super) counters: Counters,
}

pub(super) struct Entries {
    pending: Mutex<Option<Pending>>,
    calls: AtomicU64,
    requests: RequestLedger,
    durability: DurabilityQuery,
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
    pub(super) fn new(requests: RequestLedger, durability: DurabilityQuery) -> Self {
        Self {
            pending: Mutex::new(None),
            calls: AtomicU64::new(0),
            requests,
            durability,
        }
    }

    pub(super) fn arm(
        self: &Arc<Self>,
        actor: ActorReference,
        epoch: u64,
        trigger: Trigger,
    ) -> Result<(
        Scope,
        oneshot::Receiver<std::result::Result<Entered, EntryError>>,
    )> {
        let mut pending = self.pending.lock().map_err(|_| EntryError::Poisoned)?;
        if pending.is_some() {
            return Err(EntryError::ConcurrentObservation.into());
        }
        let (sender, receiver) = oneshot::channel();
        *pending = Some(Pending {
            actor,
            epoch,
            trigger,
            sender: Some(sender),
        });
        Ok((Scope(self.clone()), receiver))
    }

    pub(super) async fn receive(
        receiver: oneshot::Receiver<std::result::Result<Entered, EntryError>>,
    ) -> std::result::Result<Entered, EntryError> {
        receiver.await.map_err(|_| EntryError::ProducerDropped)?
    }

    pub(super) fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    pub(super) fn check_owned_scope(
        self: &Arc<Self>,
        actor: ActorReference,
        epoch: u64,
        trigger: Trigger,
    ) -> Result {
        match self.arm(actor, epoch, trigger) {
            Err(error)
                if error.downcast_ref::<EntryError>()
                    == Some(&EntryError::ConcurrentObservation) =>
            {
                emit(
                    serde_json::json!({"schema": 1, "kind": "negative_control", "control": "rearm after real handler entry before scope retirement", "result": "typed concurrent observation"}),
                )
            }
            _ => Err("handler-entry producer custody ended before its delivery scope".into()),
        }
    }

    pub(super) async fn check_dropped_producer(
        self: &Arc<Self>,
        actor: ActorReference,
        epoch: u64,
        trigger: Trigger,
    ) -> Result {
        let (scope, receiver) = self.arm(actor, epoch, trigger)?;
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
        let counters = Counters::capture(&self.entries.requests, &self.entries.durability);
        self.entries.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(pending) = self
            .entries
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
            && let Some(sender) = pending.sender.take()
        {
            let expected = pending.actor == delivery.actor
                && pending.epoch == delivery.epoch
                && pending.trigger == delivery.trigger;
            let _ = sender.send(if expected {
                Ok(Entered {
                    at: entered,
                    counters,
                })
            } else {
                Err(EntryError::UnexpectedDelivery)
            });
        }
        self.handler.on_record(delivery, checkpoint, cancellation)
    }
}
