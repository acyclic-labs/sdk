//! Guest-added interests share one follow per distinct stream. Manifest inputs
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

fn open<P: StreamProvider + ?Sized>(provider: &Arc<P>, path: &StreamPath, from: u64) -> Next {
    let provider = Arc::clone(provider);
    let path = path.clone();
    Box::pin(async move {
        let mut records = acyclic_stream::records_only(
            provider
                .follow(acyclic_stream::FollowRequest {
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
    _registration: Registration<StreamPath>,
    pub(super) waker: Waker,
    pub(super) next: Option<Next>,
    next_from: u64,
    notification: Option<Notification>,
    observers: BTreeMap<ActorReference, u64>,
}

#[derive(Default)]
pub(super) struct Subscriptions {
    actors: BTreeMap<ActorReference, BTreeMap<StreamPath, u64>>,
    pub(super) paths: BTreeMap<StreamPath, Watch>,
    ready: ReadySet<StreamPath>,
}

impl Subscriptions {
    pub(super) fn bind<P: StreamProvider + ?Sized>(
        &mut self,
        provider: &Arc<P>,
        manifest: &crate::guest::Manifest,
        actor: &ActorReference,
        state: &State,
    ) -> Result<(), Error> {
        let mut selected = BTreeMap::<StreamPath, u64>::new();
        for (id, subscription) in state.subscriptions() {
            if !manifest.subscriptions.contains(id) {
                selected
                    .entry(subscription.path.clone())
                    .and_modify(|cursor| *cursor = (*cursor).min(subscription.cursor))
                    .or_insert(subscription.cursor);
            }
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
        for (path, from) in &selected {
            if !self.paths.contains_key(path) {
                let registration = self.ready.register(path.clone())?;
                let waker = registration.waker();
                let next = Some(open(provider, path, *from));
                self.paths.insert(
                    path.clone(),
                    Watch {
                        _registration: registration,
                        waker,
                        next,
                        next_from: *from,
                        notification: None,
                        observers: BTreeMap::new(),
                    },
                );
            }
            let watch = self.paths.get_mut(path).expect("registered follow");
            watch.observers.insert(actor.clone(), *from);
            if watch.next.is_none() {
                watch.next = Some(open(provider, path, watch.next_from));
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

    fn remove(&mut self, path: &StreamPath, actor: &ActorReference) {
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
        let watch = self.paths.get_mut(work.key()).expect("registered source");
        if watch.notification.is_none() {
            let Some(next) = watch.next.as_mut() else {
                return Poll::Ready((Vec::new(), None));
            };
            let notification = match next.poll_unpin(&mut Context::from_waker(&watch.waker)) {
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
            };
            watch.notification = Some(notification);
        }
        let notification = watch.notification.as_mut().expect("source notification");
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
        if finished {
            watch.notification = None;
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
            Poll::Ready((actors, Some(error))) => Poll::Ready(Err((actors, error))),
            Poll::Pending => Poll::Pending,
        }
    }
}
