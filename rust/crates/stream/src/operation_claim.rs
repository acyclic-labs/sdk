//! Process-local exclusion shared by every adapter of one provider owner.

struct Node {
    key: [u8; 48],
    next: Option<Box<Node>>,
}

/// Live operation ownership independent of persisted crash-recovery records.
pub struct OperationClaims {
    active: crate::query_sync::SyncMutex<Option<Box<Node>>>,
    released: crate::query_sync::watch::Sender<()>,
}

impl Default for OperationClaims {
    fn default() -> Self {
        Self {
            active: crate::query_sync::SyncMutex::new(None),
            released: crate::query_sync::watch::channel(()).0,
        }
    }
}

/// An operation and its caller-admitted storage, released together on drop.
pub struct OperationClaim<'a, H> {
    claims: &'a OperationClaims,
    key: [u8; 48],
    _storage: H,
}

impl OperationClaims {
    /// Visits each active claim before inspecting it, then admits the exact
    /// node allocation before construction. Returns `None` for a live duplicate.
    /// The caller retains its own accounting error type and storage custody.
    pub fn claim<H, E>(
        &self,
        key: [u8; 48],
        mut visit: impl FnMut() -> Result<(), E>,
        admit: impl FnOnce(u64) -> Result<H, E>,
    ) -> Result<Option<OperationClaim<'_, H>>, E> {
        // Admission and visit callbacks run before list mutation; unwinding
        // cannot leave a partially linked node. Retain live claims on poison.
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut cursor = active.as_deref();
        while let Some(node) = cursor {
            visit()?;
            if node.key == key {
                return Ok(None);
            }
            cursor = node.next.as_deref();
        }
        let storage = admit(std::mem::size_of::<Node>() as u64)?;
        let node = Box::new(Node {
            key,
            next: active.take(),
        });
        *active = Some(node);
        Ok(Some(OperationClaim {
            claims: self,
            key,
            _storage: storage,
        }))
    }

    /// Waits for a matching live owner to release its claim, including when its
    /// future is dropped without publishing. Callers retry `claim` after waking.
    pub async fn wait_until_released(&self, key: [u8; 48]) {
        // Subscribe before inspecting ownership: a release between that check
        // and parking remains visible even when it publishes no stream record.
        let mut released = self.released.subscribe();
        loop {
            let held = {
                let active = self
                    .active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut cursor = active.as_deref();
                let mut held = false;
                while let Some(node) = cursor {
                    if node.key == key {
                        held = true;
                        break;
                    }
                    cursor = node.next.as_deref();
                }
                held
            };
            if !held || released.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for OperationClaims {
    fn drop(&mut self) {
        // A caller may forget a guard in safe Rust. Drain the remaining nodes
        // iteratively so registry destruction never recurses with claim count.
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while let Some(mut node) = active.take() {
            *active = node.next.take();
        }
    }
}

impl<H> Drop for OperationClaim<'_, H> {
    fn drop(&mut self) {
        let mut active = self
            .claims
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut cursor = &mut *active;
        loop {
            match cursor {
                Some(node) if node.key == self.key => {
                    *cursor = node.next.take();
                    drop(active);
                    self.claims.released.send_replace(());
                    return;
                }
                Some(node) => cursor = &mut node.next,
                None => unreachable!("live operation claim disappeared"),
            }
        }
    }
}

#[cfg(test)]
#[cfg(not(acyclic_loom))]
mod tests {
    use super::*;

    #[test]
    fn release_wakes_a_registered_waiter_without_a_provider_publication() {
        use std::{
            future::Future,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            task::Context,
        };
        struct Wake(AtomicUsize);
        impl futures::task::ArcWake for Wake {
            fn wake_by_ref(this: &Arc<Self>) {
                this.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let claims = OperationClaims::default();
        let owner = claims
            .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
            .unwrap()
            .unwrap();
        let wake = Arc::new(Wake(AtomicUsize::new(0)));
        let waker = futures::task::waker(wake.clone());
        let mut context = Context::from_waker(&waker);
        let mut wait = Box::pin(claims.wait_until_released([1; 48]));
        assert!(wait.as_mut().poll(&mut context).is_pending());
        drop(owner);
        assert!(wake.0.load(Ordering::SeqCst) > 0);
        assert!(wait.as_mut().poll(&mut context).is_ready());
        // A release before the first poll must not be lost either.
        let mut after_release = Box::pin(claims.wait_until_released([1; 48]));
        assert!(after_release.as_mut().poll(&mut context).is_ready());
    }

    #[test]
    fn panicking_admission_preserves_live_ownership_and_retirement() {
        let claims = OperationClaims::default();
        let original = claims
            .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
            .unwrap()
            .unwrap();
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = claims.claim(
                [2; 48],
                || Ok::<_, ()>(()),
                |_| -> Result<(), ()> { panic!("admission failed") },
            );
        }));
        assert!(failed.is_err());
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_none()
        );
        assert!(
            claims
                .claim([2; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
        drop(original);
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
    }
}
