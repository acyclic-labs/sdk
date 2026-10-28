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
    changes: Shared<Changes>,
    id: Shared<()>,
    receive: oneshot::Receiver<()>,
}

impl CredentialWatch {
    /// Source-owned observation captured while registering this notification.
    pub fn validity(&self) -> CredentialValidity {
        self.validity
    }
}

impl Future for CredentialWatch {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.receive).poll(cx).map(|_| ())
    }
}

impl Drop for CredentialWatch {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.changes.0.lock() {
            pending.remove(&(Shared::as_ptr(&self.id) as usize));
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
            changes: self.clone(),
            id,
            receive,
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
