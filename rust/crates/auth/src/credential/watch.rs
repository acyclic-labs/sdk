use super::{CredentialCacheError, CredentialValidity, Shared};
use futures::channel::oneshot;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

#[derive(Default)]
pub(super) struct Changes(Mutex<BTreeMap<usize, oneshot::Sender<()>>>);

/// One source observation and a one-shot change notification. Subscribe before
/// reading authority-bound cached data. Await this to cancel outstanding work,
/// discard that data and subscribe again. Drop unregisters without a task.
/// Expiry still requires the admitted lease's absolute deadline/check; this
/// signal has no timer and does not turn a revision into a permission.
pub struct CredentialWatch {
    validity: CredentialValidity,
    registration: Registration,
}

#[cfg(not(target_arch = "wasm32"))]
type WatchFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
#[cfg(target_arch = "wasm32")]
type WatchFuture = Pin<Box<dyn Future<Output = ()>>>;

enum Registration {
    Cached {
        changes: Shared<Changes>,
        id: Shared<()>,
        receive: oneshot::Receiver<()>,
    },
    Custodian(WatchFuture),
}

impl CredentialWatch {
    /// Source-owned observation captured while registering this notification.
    pub fn validity(&self) -> CredentialValidity {
        self.validity.clone()
    }

    /// Retain the original custodian's registration. The future must unregister
    /// on drop and complete on change or loss; no task or expiry timer is added.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_custodian(
        validity: CredentialValidity,
        changed: impl Future<Output = ()> + Send + 'static,
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
        changed: impl Future<Output = ()> + 'static,
    ) -> Self {
        Self {
            validity,
            registration: Registration::Custodian(Box::pin(changed)),
        }
    }
}

impl Future for CredentialWatch {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match &mut self.registration {
            Registration::Cached { receive, .. } => Pin::new(receive).poll(cx).map(|_| ()),
            Registration::Custodian(changed) => changed.as_mut().poll(cx),
        }
    }
}

impl Drop for CredentialWatch {
    fn drop(&mut self) {
        if let Registration::Cached { changes, id, .. } = &self.registration {
            // Drop the sender outside the lock: waking can reenter the source.
            let removed = changes
                .0
                .lock()
                .ok()
                .and_then(|mut pending| pending.remove(&(Shared::as_ptr(id) as usize)));
            drop(removed);
        }
    }
}

impl Changes {
    pub(super) fn watch(
        self: &Shared<Self>,
        observe: impl FnOnce() -> CredentialValidity,
    ) -> Result<CredentialWatch, CredentialCacheError> {
        let mut pending = self
            .0
            .lock()
            .map_err(|_| CredentialCacheError::Unavailable)?;
        let validity = observe();
        let id = Shared::new(());
        let (send, receive) = oneshot::channel();
        pending.insert(Shared::as_ptr(&id) as usize, send);
        Ok(CredentialWatch {
            validity,
            registration: Registration::Cached {
                changes: self.clone(),
                id,
                receive,
            },
        })
    }

    pub(super) fn notify(&self) -> Result<(), CredentialCacheError> {
        let pending = std::mem::take(
            &mut *self
                .0
                .lock()
                .map_err(|_| CredentialCacheError::Unavailable)?,
        );
        // Wakers may reenter the source. Wake only after releasing its lock.
        for (_, send) in pending {
            let _ = send.send(());
        }
        Ok(())
    }
}
