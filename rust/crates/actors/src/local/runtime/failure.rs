//! Actor-scoped failure events, retained only by resident actors or observers.
use super::{ActorReference, Arc, BoxProviderFuture, DeliveryFailure, HashMap, SyncMutex};
use std::sync::Weak;

pub(super) struct Signals {
    index: SyncMutex<HashMap<ActorReference, Weak<Signal>>>,
    alive: tokio::sync::watch::Sender<()>,
}

impl Default for Signals {
    fn default() -> Self {
        Self {
            index: SyncMutex::new(HashMap::new()),
            alive: tokio::sync::watch::Sender::new(()),
        }
    }
}

impl Signals {
    pub(super) fn existing(&self, actor: &ActorReference) -> Option<Arc<Signal>> {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(actor)
            .and_then(Weak::upgrade)
    }

    pub(super) fn signal(self: &Arc<Self>, actor: &ActorReference) -> Arc<Signal> {
        let mut signals = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(signal) = signals.get(actor).and_then(Weak::upgrade) {
            return signal;
        }
        let signal = Arc::new(Signal {
            actor: actor.clone(),
            signals: Arc::downgrade(self),
            state: tokio::sync::watch::Sender::new(State::default()),
            lifetime: self.alive.subscribe(),
        });
        signals.insert(actor.clone(), Arc::downgrade(&signal));
        signal
    }
}

#[derive(Default)]
struct State {
    latest: Option<Arc<DeliveryFailure>>,
    active: std::collections::BTreeMap<ActorReference, Arc<DeliveryFailure>>,
}

pub(super) struct Signal {
    actor: ActorReference,
    signals: Weak<Signals>,
    state: tokio::sync::watch::Sender<State>,
    lifetime: tokio::sync::watch::Receiver<()>,
}

impl Signal {
    pub(super) fn failed(&self, actor: &ActorReference, cause: Arc<DeliveryFailure>) {
        self.state.send_modify(|state| {
            state.latest = Some(Arc::clone(&cause));
            state.active.insert(actor.clone(), cause);
        });
    }

    pub(super) fn healthy(&self, actor: &ActorReference) {
        self.state
            .send_if_modified(|state| state.active.remove(actor).is_some());
    }

    pub(super) fn observe(self: Arc<Self>) -> BoxProviderFuture<'static, DeliveryFailure> {
        let mut updates = self.state.subscribe();
        let mut lifetime = self.lifetime.clone();
        let (observed, active) = {
            let state = updates.borrow_and_update();
            (state.latest.clone(), state.active.values().next().cloned())
        };
        Box::pin(async move {
            // Keep the sender and its weak index alive across slot release.
            // A new resident slot finds this same signal; actor I/O is freed.
            let _signal = self;
            if let Some(cause) = active {
                return cause.as_ref().clone();
            }
            loop {
                let current = updates.borrow_and_update().latest.clone();
                if let Some(cause) = current
                    && observed
                        .as_ref()
                        .is_none_or(|seen| !Arc::ptr_eq(seen, &cause))
                {
                    return cause.as_ref().clone();
                }
                tokio::select! {
                    biased;
                    changed = updates.changed() => {
                        if changed.is_err() {
                            return DeliveryFailure { message: format!("actor {:?}: failure producer closed", _signal.actor) };
                        }
                    },
                    _ = lifetime.changed() => return DeliveryFailure {
                        message: format!("actor {:?}: runtime released", _signal.actor),
                    },
                }
            }
        })
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        if let Some(signals) = self.signals.upgrade() {
            let mut index = signals
                .index
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
