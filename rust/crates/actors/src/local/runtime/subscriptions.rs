//! Guest-added interests share one follow per distinct stream and selection. Manifest inputs
//! keep producer-owned activation. The existing ready set coalesces source
//! events and retires their I/O immediately when the last observer leaves.
use super::*;
use futures::TryStreamExt;

type Next = BoxProviderFuture<'static, Result<(u64, RecordStream), StreamError>>;

struct Notification {
    through: u64,
    after: Option<ActorReference>,
    error: Option<StreamError>,
}

fn open<P: StreamProvider + ?Sized>(
    provider: &Arc<P>,
    path: &StreamPath,
    filter: Option<acyclic_stream::FollowFilter>,
    from: u64,
) -> Next {
    let provider = Arc::clone(provider);
    let path = path.clone();
    Box::pin(async move {
        let mut records = acyclic_stream::records_only(
            provider
                .follow(acyclic_stream::FollowRequest {
                    filter: filter.unwrap_or_default(),
                    when_absent: acyclic_stream::WhenAbsent::Wait,
                    ..acyclic_stream::FollowRequest::new(path, from)
                })
                .await?,
        );
        let record = records.try_next().await?.ok_or(StreamError::Unavailable)?;
        Ok((record.sequence, records))
    })
}

pub(super) struct Watch {
    _registration: Registration<Source>,
    filter: Option<acyclic_stream::FollowFilter>,
    pub(super) waker: Waker,
    pub(super) next: Option<Next>,
    next_from: u64,
    notification: Option<Notification>,
    observers: BTreeMap<ActorReference, u64>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Source {
    pub(super) path: StreamPath,
    filter: Option<[u8; 32]>,
}

#[derive(Clone, PartialEq)]
pub(super) struct Selection {
    cursor: u64,
    filter: Option<acyclic_stream::FollowFilter>,
}

pub(super) type Selections = BTreeMap<Source, Selection>;

#[derive(Default)]
pub(super) struct Subscriptions {
    actors: BTreeMap<ActorReference, Selections>,
    pub(super) paths: BTreeMap<Source, Watch>,
    ready: ReadySet<Source>,
}

impl Subscriptions {
    /// The guest-added interests of `state`, each at its earliest cursor.
    pub(super) fn selected(handler: &dyn Handler, state: &State) -> Result<Selections, Error> {
        let mut selected = BTreeMap::<Source, Selection>::new();
        for (id, subscription) in state.subscriptions() {
            if !subscription.paused && !handler.manifest().subscriptions.contains(id) {
                let filter = indexed_filter(handler, id, state.checkpoint())?;
                let source = Source {
                    path: subscription.path.clone(),
                    filter: filter.as_ref().map(acyclic_stream::FollowFilter::digest),
                };
                match selected.entry(source) {
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if entry.get().filter != filter {
                            return Err(Error::Invalid("subscription filter identity collision"));
                        }
                        let cursor = entry.get().cursor.min(subscription.cursor);
                        entry.get_mut().cursor = cursor;
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(Selection {
                            cursor: subscription.cursor,
                            filter,
                        });
                    }
                }
            }
        }
        Ok(selected)
    }

    /// The interest streams of `state` that no follow observes yet.
    pub(super) fn unwatched(&self, selected: &Selections) -> Vec<StreamPath> {
        selected
            .keys()
            .filter(|source| !self.paths.contains_key(source))
            .map(|source| source.path.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Bind `actor`'s interests. A new follow starts at the tail its binding
    /// delivery read before looking for input (`tails`): that delivery owns
    /// every earlier record, so the follow wakes only for later input and a
    /// terminally failed input is never redelivered without a new event.
    pub(super) fn bind<P: StreamProvider + ?Sized>(
        &mut self,
        provider: &Arc<P>,
        actor: &ActorReference,
        selected: Selections,
        tails: &BTreeMap<StreamPath, u64>,
    ) -> Result<(), Error> {
        if selected.iter().any(|(source, selection)| {
            self.paths
                .get(source)
                .is_some_and(|watch| watch.filter != selection.filter)
        }) {
            return Err(Error::Invalid("subscription filter identity collision"));
        }
        if self.actors.get(actor) == Some(&selected)
            && selected.keys().all(|path| {
                self.paths
                    .get(path)
                    .is_some_and(|watch| watch.next.is_some())
            })
        {
            return Ok(());
        }
        let previous = self.actors.remove(actor).unwrap_or_default();
        for path in previous.keys().filter(|path| !selected.contains_key(*path)) {
            self.remove(path, actor);
        }
        for (path, selection) in &selected {
            let watch = match self.paths.entry(path.clone()) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let registration = self.ready.register(path.clone())?;
                    let waker = registration.waker();
                    entry.insert(Watch {
                        _registration: registration,
                        filter: selection.filter.clone(),
                        waker,
                        next: None,
                        next_from: tails
                            .get(&path.path)
                            .map_or(selection.cursor, |&tail| tail.max(selection.cursor)),
                        notification: None,
                        observers: BTreeMap::new(),
                    })
                }
            };
            watch.observers.insert(actor.clone(), selection.cursor);
            // A registration starts idle: every follow this opens is armed
            // through its own wake, the first one included. Arming polls the
            // follow; only a record it returns wakes an observer.
            if watch.next.is_none() {
                watch.next = Some(open(
                    provider,
                    &path.path,
                    watch.filter.clone(),
                    watch.next_from,
                ));
                watch.waker.wake_by_ref();
            }
        }
        if !selected.is_empty() {
            self.actors.insert(actor.clone(), selected);
        }
        Ok(())
    }

    pub(super) fn release(&mut self, actor: &ActorReference) {
        if let Some(paths) = self.actors.remove(actor) {
            for path in paths.keys() {
                self.remove(path, actor);
            }
        }
    }

    fn remove(&mut self, path: &Source, actor: &ActorReference) {
        if let Some(watch) = self.paths.get_mut(path) {
            watch.observers.remove(actor);
            if watch.observers.is_empty() {
                self.paths.remove(path);
            }
        }
    }

    pub(super) fn poll(
        &mut self,
        context: &mut Context<'_>,
        limit: usize,
    ) -> Poll<(Vec<ActorReference>, Option<StreamError>)> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.ready.listen(context);
        let Some(work) = self.ready.take() else {
            return Poll::Pending;
        };
        let Some(watch) = self.paths.get_mut(work.key()) else {
            return Poll::Ready((Vec::new(), Some(StreamError::InvalidResponse)));
        };
        let mut notification = match watch.notification.take() {
            Some(notification) => notification,
            None => {
                let Some(next) = watch.next.as_mut() else {
                    return Poll::Ready((Vec::new(), None));
                };
                match next.poll_unpin(&mut Context::from_waker(&watch.waker)) {
                    Poll::Pending => return Poll::Ready((Vec::new(), None)),
                    Poll::Ready(Ok((sequence, mut records))) => match sequence.checked_add(1) {
                        Some(from) => {
                            watch.next_from = from;
                            watch.next = Some(Box::pin(async move {
                                let record =
                                    records.try_next().await?.ok_or(StreamError::Unavailable)?;
                                Ok((record.sequence, records))
                            }));
                            Notification {
                                through: sequence,
                                after: None,
                                error: None,
                            }
                        }
                        None => {
                            watch.next = None;
                            Notification {
                                through: u64::MAX,
                                after: None,
                                error: Some(StreamError::InvalidResponse),
                            }
                        }
                    },
                    Poll::Ready(Err(error)) => {
                        watch.next = None;
                        Notification {
                            through: u64::MAX,
                            after: None,
                            error: Some(error),
                        }
                    }
                }
            }
        };
        let after = notification.after.clone();
        let mut ready = Vec::new();
        let mut scanned = 0;
        for (actor, cursor) in watch
            .observers
            .range((after.as_ref().map_or(Unbounded, Excluded), Unbounded))
            .take(limit)
        {
            notification.after = Some(actor.clone());
            scanned += 1;
            if notification.error.is_some() || *cursor <= notification.through {
                ready.push(actor.clone());
            }
        }
        let error = notification.error.clone();
        let finished = scanned < limit
            || notification.after.as_ref()
                == watch.observers.last_key_value().map(|(actor, _)| actor);
        if !finished {
            watch.notification = Some(notification);
        }
        if !finished || watch.next.is_some() {
            watch.waker.wake_by_ref();
        }
        Poll::Ready((ready, error))
    }
}
impl<P: StreamProvider + ?Sized> Runtime<P> {
    pub(super) fn poll_subscriptions(
        &self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), (Vec<ActorReference>, StreamError)>> {
        let result = self
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .poll(context, self.limits.work);
        match result {
            Poll::Ready((actors, None)) => {
                for actor in actors {
                    self.wake(&actor);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready((actors, Some(error))) => {
                for actor in &actors {
                    self.subscription_failed(actor, &error);
                }
                Poll::Ready(Err((actors, error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
