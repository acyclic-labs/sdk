//! External source follows are shared dispatcher inputs, never per-actor tasks.
//! Their read cursors schedule wakes only; committed actor cursors own delivery.
use super::*;
use crate::guest::Subscription;
use futures::task::AtomicWaker;
use std::collections::VecDeque;
use tokio::sync::watch;
#[cfg(test)]
mod tests;

#[derive(Clone, PartialEq, Eq)]
struct Selection {
    subscription: Subscription,
    key: Option<bytes::Bytes>,
}

#[derive(Default)]
struct Registrations {
    running: bool,
    actors: HashMap<ActorReference, BTreeMap<String, watch::Sender<Selection>>>,
    pending: VecDeque<Watch>,
}

pub(super) struct Running<'a>(&'a Registry);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        let mut registrations = self
            .0
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registrations.running = false;
        let retired = (
            std::mem::take(&mut registrations.actors),
            std::mem::take(&mut registrations.pending),
        );
        drop(registrations);
        drop(retired);
        self.0.changed.take();
    }
}

#[derive(Default)]
pub(super) struct Registry {
    registrations: SyncMutex<Registrations>,
    changed: AtomicWaker,
}

impl Registry {
    pub(super) fn running(&self) -> Running<'_> {
        self.registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .running = true;
        Running(self)
    }
    pub(super) fn remove(&self, actor: &ActorReference) {
        self.registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .actors
            .remove(actor);
    }

    pub(super) fn refresh(
        &self,
        actor: &ActorReference,
        state: &State,
        handler: &dyn Handler,
        wake: &Waker,
    ) -> Result<(), Error> {
        let ids = handler.external_subscriptions();
        if ids.is_empty() {
            return Ok(());
        }
        if state.paused() {
            self.remove(actor);
            return Ok(());
        }
        let selections = ids
            .iter()
            .filter_map(|id| state.subscriptions().get(id).map(|cut| (id, cut)))
            .map(|(id, cut)| {
                Ok((
                    id,
                    Selection {
                        subscription: cut.clone(),
                        key: handler.subscription_key(id, state.checkpoint())?,
                    },
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut registrations = self
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !registrations.running {
            return Ok(());
        }
        let mut entries = registrations.actors.remove(actor).unwrap_or_default();
        entries.retain(|id, entry| {
            ids.contains(id) && state.subscriptions().contains_key(id) && !entry.is_closed()
        });
        for (id, selection) in selections {
            match entries.get(id) {
                Some(entry) => {
                    entry.send_if_modified(|current| {
                        if *current == selection {
                            return false;
                        }
                        *current = selection.clone();
                        true
                    });
                }
                None => {
                    let (send, receive) = watch::channel(selection.clone());
                    entries.insert(id.clone(), send);
                    registrations.pending.push_back(Watch {
                        actor: actor.clone(),
                        receive,
                        next: selection.subscription.cursor,
                        selected: selection,
                        records: None,
                        wake: wake.clone(),
                    });
                    self.changed.wake();
                }
            }
        }
        if !entries.is_empty() {
            registrations.actors.insert(actor.clone(), entries);
        }
        Ok(())
    }

    pub(super) fn poll_registered(&self, context: &mut Context<'_>) -> Poll<Watch> {
        self.changed.register(context.waker());
        self.registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .pop_front()
            .map_or(Poll::Pending, Poll::Ready)
    }
}

pub(super) struct Watch {
    actor: ActorReference,
    receive: watch::Receiver<Selection>,
    selected: Selection,
    next: u64,
    records: Option<RecordStream>,
    wake: Waker,
}

pub(super) struct Outcome {
    pub(super) watch: Option<Watch>,
    pub(super) result: Result<(), (ActorReference, StreamError)>,
}

impl Watch {
    fn update(&mut self) {
        let next = self.receive.borrow_and_update().clone();
        if next.subscription.path != self.selected.subscription.path
            || next.key != self.selected.key
            || next.subscription.cursor < self.selected.subscription.cursor
            || next.subscription.cursor > self.next
        {
            self.next = next.subscription.cursor;
            self.records = None;
        }
        self.selected = next;
    }

    pub(super) fn next<'a, P: StreamProvider + ?Sized>(
        mut self,
        provider: &'a Arc<P>,
    ) -> BoxProviderFuture<'a, Outcome> {
        Box::pin(async move {
            let result = async {
                loop {
                    self.update();
                    if self.records.is_none() {
                        let mut request = acyclic_stream::FollowRequest::new(self.selected.subscription.path.clone(), self.next);
                        if let Some(key) = &self.selected.key {
                            request.filter = acyclic_stream::FollowFilter::exact_key(key.clone())?;
                        }
                        request.when_absent = acyclic_stream::WhenAbsent::Wait;
                        let open = provider.follow(request);
                        tokio::pin!(open);
                        tokio::select! {
                            biased;
                            changed = self.receive.changed() => {
                                if changed.is_err() { return Ok(false); }
                                continue;
                            }
                            opened = &mut open => {
                                self.records = Some(acyclic_stream::records_only(opened?));
                            }
                        }
                    }
                    let records = self.records.as_mut().ok_or(StreamError::Unavailable)?;
                    tokio::select! {
                        biased;
                        changed = self.receive.changed() => {
                            if changed.is_err() { return Ok(false); }
                        }
                        record = records.next() => {
                            let record = record.ok_or(StreamError::Unavailable)??;
                            self.next = record.sequence.checked_add(1).ok_or(StreamError::LimitExceeded)?;
                            self.wake.wake_by_ref();
                            return Ok(true);
                        }
                    }
                }
            }.await;
            match result {
                Ok(true) => Outcome {
                    watch: Some(self),
                    result: Ok(()),
                },
                Ok(false) => Outcome {
                    watch: None,
                    result: Ok(()),
                },
                Err(error) => Outcome {
                    watch: None,
                    result: Err((self.actor, error)),
                },
            }
        })
    }
}
