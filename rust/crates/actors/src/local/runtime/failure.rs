//! Actor-scoped failure events, retained only by resident actors or observers.
use super::{ActorReference, Arc, BoxProviderFuture, DeliveryFailure, HashMap, SyncMutex};
use std::sync::Weak;

#[derive(Default)]
pub(super) struct Signals(SyncMutex<HashMap<ActorReference, Weak<Signal>>>);

impl Signals {
    pub(super) fn signal(self: &Arc<Self>, actor: &ActorReference) -> Arc<Signal> {
        let mut signals = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(signal) = signals.get(actor).and_then(Weak::upgrade) {
            return signal;
        }
        let signal = Arc::new(Signal {
            actor: actor.clone(),
            signals: Arc::downgrade(self),
            state: tokio::sync::watch::Sender::new(State::default()),
        });
        signals.insert(actor.clone(), Arc::downgrade(&signal));
        signal
    }
}

#[derive(Clone, Default)]
struct State {
    latest: Option<Arc<DeliveryFailure>>,
    failed: bool,
}

pub(super) struct Signal {
    actor: ActorReference,
    signals: Weak<Signals>,
    state: tokio::sync::watch::Sender<State>,
}

impl Signal {
    pub(super) fn failed(&self, cause: DeliveryFailure) {
        self.state.send_replace(State {
            latest: Some(Arc::new(cause)),
            failed: true,
        });
    }

    pub(super) fn healthy(&self) {
        self.state
            .send_if_modified(|state| std::mem::take(&mut state.failed));
    }

    pub(super) fn observe(self: Arc<Self>) -> BoxProviderFuture<'static, DeliveryFailure> {
        let mut updates = self.state.subscribe();
        let observed = updates.borrow_and_update().clone();
        Box::pin(async move {
            // Keep the sender and its weak index alive across slot release.
            // A new resident slot finds this same signal; actor I/O is freed.
            let _signal = self;
            if observed.failed {
                return observed
                    .latest
                    .expect("failed state has a cause")
                    .as_ref()
                    .clone();
            }
            loop {
                let current = updates.borrow_and_update().clone();
                if let Some(cause) = current.latest
                    && observed
                        .latest
                        .as_ref()
                        .is_none_or(|seen| !Arc::ptr_eq(seen, &cause))
                {
                    return cause.as_ref().clone();
                }
                updates
                    .changed()
                    .await
                    .expect("observer retains its sender");
            }
        })
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        if let Some(signals) = self.signals.upgrade() {
            let mut index = signals
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if index
                .get(&self.actor)
                .is_some_and(|signal| std::ptr::eq(signal.as_ptr(), self))
            {
                index.remove(&self.actor);
            }
        }
    }
}
