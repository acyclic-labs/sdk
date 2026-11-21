//! Readers can wait for writer progress without reserving a shared permit.
use crate::query_sync::{RwLock, watch};
use std::ops::{Deref, DerefMut};

pub(crate) struct ObservationLock<T> {
    lock: RwLock<T>,
    released: watch::Sender<()>,
}

impl<T> ObservationLock<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            lock: RwLock::new(value),
            released: watch::channel(()).0,
        }
    }

    pub(crate) async fn read_with(&self, unreserved: bool) -> impl Deref<Target = T> + '_ {
        if !unreserved {
            return ReadGuard::Reserved(self.lock.read().await);
        }
        if let Ok(guard) = self.lock.try_read() {
            return ReadGuard::Unreserved(guard);
        }
        // Subscribe before the second try to cover a release racing admission.
        let mut released = self.released.subscribe();
        loop {
            if let Ok(guard) = self.lock.try_read() {
                return ReadGuard::Unreserved(guard);
            }
            released
                .changed()
                .await
                .unwrap_or_else(|_| unreachable!("the lock owns the sender"));
        }
    }

    pub(crate) fn read(&self) -> impl Future<Output = impl Deref<Target = T> + '_> + '_ {
        self.lock.read()
    }

    pub(crate) async fn write(&self) -> impl DerefMut<Target = T> + '_ {
        // Cancellation of a queued writer can reopen admission too. Dropping
        // the acquisition or held guard always precedes the notification.
        let released = Released(&self.released);
        let guard = self.lock.write().await;
        WriteGuard {
            guard,
            _released: released,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_write(&self) -> Result<impl DerefMut<Target = T> + '_, ()> {
        self.lock
            .try_write()
            .map_err(|_| ())
            .map(|guard| WriteGuard {
                guard,
                _released: Released(&self.released),
            })
    }
}

struct Released<'a>(&'a watch::Sender<()>);

impl Drop for Released<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|_| {});
    }
}

struct WriteGuard<'a, G> {
    guard: G,
    _released: Released<'a>,
}

impl<G: Deref> Deref for WriteGuard<'_, G> {
    type Target = G::Target;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<G: DerefMut> DerefMut for WriteGuard<'_, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

enum ReadGuard<R, U> {
    Reserved(R),
    Unreserved(U),
}

impl<T, R: Deref<Target = T>, U: Deref<Target = T>> Deref for ReadGuard<R, U> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            Self::Reserved(guard) => guard,
            Self::Unreserved(guard) => guard,
        }
    }
}
