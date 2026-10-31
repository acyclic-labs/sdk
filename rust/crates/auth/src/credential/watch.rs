use super::{CredentialCacheError, CredentialValidity};
use futures::{FutureExt, channel::oneshot, future::Shared};
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

type Change = Shared<oneshot::Receiver<()>>;

struct Epoch {
    send: oneshot::Sender<()>,
    changed: Change,
}

impl Default for Epoch {
    fn default() -> Self {
        let (send, receive) = oneshot::channel();
        Self {
            send,
            changed: receive.shared(),
        }
    }
}

#[derive(Default)]
pub(super) struct Changes(Mutex<Option<Epoch>>);

/// One source observation and a one-shot change notification. Subscribe before
/// reading authority-bound cached data. Await this to cancel outstanding work,
/// discard that data and subscribe again. Dropping the source completes with
/// `CredentialCacheError::Unavailable`; a watch never owns its event producer.
/// Expiry still requires the admitted lease's absolute deadline/check; this
/// signal has no timer and does not turn a revision into a permission.
pub struct CredentialWatch {
    validity: CredentialValidity,
    registration: Registration,
}

#[cfg(not(target_arch = "wasm32"))]
type WatchFuture = Pin<Box<dyn Future<Output = Result<(), CredentialCacheError>> + Send>>;
#[cfg(target_arch = "wasm32")]
type WatchFuture = Pin<Box<dyn Future<Output = Result<(), CredentialCacheError>>>>;

enum Registration {
    Cached(Change),
    Custodian(WatchFuture),
}

impl CredentialWatch {
    /// Source-owned observation captured while registering this notification.
    pub fn validity(&self) -> CredentialValidity {
        self.validity.clone()
    }

    /// Retain the original custodian's registration. The future must unregister
    /// on drop, complete on change and report source loss as a typed error.
    /// No task or expiry timer is added.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_custodian(
        validity: CredentialValidity,
        changed: impl Future<Output = Result<(), CredentialCacheError>> + Send + 'static,
    ) -> Self {
        Self {
            validity,
            registration: Registration::Custodian(Box::pin(changed)),
        }
    }

    /// Browser registration may own a JavaScript abort subscription.
    #[cfg(target_arch = "wasm32")]
    pub fn from_custodian(
        validity: CredentialValidity,
        changed: impl Future<Output = Result<(), CredentialCacheError>> + 'static,
    ) -> Self {
        Self {
            validity,
            registration: Registration::Custodian(Box::pin(changed)),
        }
    }
}

impl Future for CredentialWatch {
    type Output = Result<(), CredentialCacheError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.registration {
            Registration::Cached(receive) => Pin::new(receive)
                .poll(cx)
                .map(|result| result.map_err(|_| CredentialCacheError::Unavailable)),
            Registration::Custodian(changed) => changed.as_mut().poll(cx),
        }
    }
}

impl Changes {
    pub(super) fn watch(
        &self,
        observe: impl FnOnce() -> CredentialValidity,
    ) -> Result<CredentialWatch, CredentialCacheError> {
        let changed = self
            .0
            .lock()
            .map_err(|_| CredentialCacheError::Unavailable)?
            .get_or_insert_with(Epoch::default)
            .changed
            .clone();
        // Subscribe before observing. A concurrent or reentrant invalidation
        // completes this watch even if observation already sees the new state.
        // The source callback never runs under the notification lock.
        let validity = observe();
        Ok(CredentialWatch {
            validity,
            registration: Registration::Cached(changed),
        })
    }

    pub(super) fn notify(&self) -> Result<(), CredentialCacheError> {
        let previous = self
            .0
            .lock()
            .map_err(|_| CredentialCacheError::Unavailable)?
            .take();
        // A waker may reenter the source. Detach this epoch before delivery;
        // only a new subscription needs to allocate the next event producer.
        if let Some(previous) = previous {
            let _ = previous.send.send(());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observation_can_publish_change_without_locking_or_losing_the_event() {
        let changes = Changes::default();
        let observe = || CredentialValidity {
            now_unix_seconds: 1,
            revision: None,
        };
        let watch = changes
            .watch(|| {
                // Refuse a regressed lock placement before reentry could deadlock.
                assert!(
                    changes.0.try_lock().is_ok(),
                    "source observation holds notification lock"
                );
                changes.notify().unwrap();
                observe()
            })
            .unwrap();
        assert!(matches!(watch.now_or_never(), Some(Ok(()))));
        let mut next = Box::pin(changes.watch(observe).unwrap());
        assert!(next.as_mut().now_or_never().is_none());
        drop(changes);
        assert!(matches!(
            next.now_or_never(),
            Some(Err(CredentialCacheError::Unavailable))
        ));
    }
}
