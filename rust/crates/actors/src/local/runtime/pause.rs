//! Owned pause decisions share the original placement and delivery fence.
use super::*;

/// The error stays in the placement until its observer takes it. Dropping an
/// unread receipt releases the control lock without discarding that error.
struct Failure {
    slot: Arc<Slot>,
    acknowledgement: futures::channel::oneshot::Sender<()>,
}

impl Failure {
    fn take(self) -> Error {
        let error = self
            .slot
            .delivery_control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .take();
        // The original control owner retains its lock through this take, so
        // another control cannot consume a living observer's result.
        drop(self.acknowledgement);
        error.map_or(Error::Invalid("pause outcome was already taken"), |error| {
            *error
        })
    }
}

pub(super) fn start<P, T>(
    store: Store<P>,
    actor: ActorReference,
    placement: EventDelivery,
    paused: bool,
    transition: impl std::future::Future<Output = T> + acyclic_stream::ProviderTask + 'static,
) -> impl std::future::Future<Output = Result<T, Error>>
where
    P: StreamProvider + ?Sized + 'static,
    T: acyclic_stream::ProviderTask + 'static,
{
    let (send, receive) = futures::channel::oneshot::channel();
    PlatformExecutor.spawn_owned(Box::pin(async move {
        let slot = &placement.0;
        let _control = slot.control.lock().await;
        match apply(&store, &actor, slot, paused, transition).await {
            Ok(value) => {
                let _ = send.send(Ok(value));
            }
            Err(()) => {
                let (acknowledgement, taken) = futures::channel::oneshot::channel();
                let _ = send.send(Err(Failure {
                    slot: Arc::clone(slot),
                    acknowledgement,
                }));
                // The same waiter either takes its result or drops its receipt.
                // Both close this channel; no independent producer is needed.
                let _ = taken.await;
            }
        }
    }));
    async move {
        match receive.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(failure)) => Err(failure.take()),
            Err(_) => Err(Error::Stream(StreamError::Unavailable)),
        }
    }
}

/// The tentative gate is scoped to the owned decision, not its observer.
/// On failure, invalidate the resident cut before reopening admission: an
/// unknown publication may already have changed the durable pause state.
struct Gate<'a> {
    slot: &'a Slot,
    decided: bool,
}

impl Drop for Gate<'_> {
    fn drop(&mut self) {
        self.slot.refresh.store(true, Ordering::Release);
        self.slot
            .delivery_control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .paused = self.decided;
        self.slot.waker.wake_by_ref();
    }
}

pub(super) async fn apply<P: StreamProvider + ?Sized, T>(
    store: &Store<P>,
    actor: &ActorReference,
    slot: &Slot,
    paused: bool,
    transition: impl std::future::Future<Output = T>,
) -> Result<T, ()> {
    let (mut gate, cancellation) = {
        let mut control = slot
            .delivery_control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if control.failure.is_some() {
            return Err(());
        }
        let gate = Gate {
            slot,
            decided: control.paused,
        };
        if paused {
            control.paused = true;
        }
        let cancellation = paused.then(|| control.cancellation.clone()).flatten();
        (gate, cancellation)
    };
    if let Some(cancellation) = cancellation {
        cancellation.cancel();
    }
    // The original delivery retains this lock through physical cleanup and
    // publication settlement. A later control cannot pass either transition.
    let delivery = slot.inner.lock().await;
    let mut epoch = (delivery.epoch != 0).then_some(delivery.epoch);
    let result = async {
        loop {
            let snapshot = store.load(actor).await?;
            let state = snapshot
                .state()
                .ok_or(Error::Invalid("actor state was never created"))?;
            if epoch.is_some_and(|epoch| epoch != state.epoch()) {
                return Err(Error::Conflict);
            }
            epoch = Some(state.epoch());
            gate.decided = state.paused();
            if state.paused() == paused {
                break;
            }
            let operation = if paused { "pause" } else { "resume" };
            match store
                .set_paused(&snapshot, state.epoch(), paused, key(&snapshot, operation)?)
                .await
            {
                Err(Error::Conflict) => {}
                result => {
                    result?;
                    gate.decided = paused;
                    break;
                }
            }
        }
        Ok(transition.await)
    }
    .await;
    let result = match result {
        Ok(value) => Ok(value),
        Err(error) => {
            // Publish error custody before reopening delivery. Its waiter and
            // later controls use this same short control lock, never the
            // delivery lock an unrelated handler/provider may retain.
            slot.delivery_control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .failure = Some(Box::new(error));
            Err(())
        }
    };
    // Keep delivery custody until resource transition completion, then publish
    // the gate decision before allowing a queued delivery to acquire the lock.
    drop(gate);
    drop(delivery);
    result
}
