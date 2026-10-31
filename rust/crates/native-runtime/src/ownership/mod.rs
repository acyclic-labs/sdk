use crate::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use crate::*;

/// Shared lifetime token for native resources that must not outlive an external owner.
pub struct OwnershipAnchor {
    _owner: Arc<dyn RetainFence>,
    release: OwnershipReleaseBarrier,
}

impl Clone for OwnershipAnchor {
    fn clone(&self) -> Self {
        Self {
            _owner: Arc::clone(&self._owner),
            release: self.release_barrier(),
        }
    }
}

// Field order is the boundary: final shared-owner cleanup precedes every fence.
// Lock order: reopen fence -> `fences`; retain never acquires a reopen lock.
// Final cleanup drops the fence list without locking it.
struct SharedOwner<Owner, Fence> {
    _owner: Owner,
    fences: crate::sync::Mutex<Vec<Fence>>,
    // Last: signal after the owner's destructor and every retained fence.
    release: ReleaseSignal,
}

impl<Owner, Fence> SharedOwner<Owner, Fence> {
    fn new(owner: Owner) -> Self {
        Self {
            _owner: owner,
            fences: crate::sync::Mutex::new(Vec::new()),
            release: ReleaseSignal(Arc::new(ReleaseState {
                complete: AtomicBool::new(false),
                blocking_lock: Mutex::new(()),
                blocking: Condvar::new(),
                asynchronous: tokio::sync::Notify::new(),
            })),
        }
    }

    fn retain(&self, fence: Fence) {
        self.fences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(fence);
    }
}

trait RetainFence: Send + Sync {
    fn retain_fence(&self, fence: Box<dyn Send + Sync>);
}

impl<T: Send + Sync> RetainFence for SharedOwner<T, Box<dyn Send + Sync>> {
    fn retain_fence(&self, fence: Box<dyn Send + Sync>) {
        self.retain(fence);
    }
}

impl OwnershipAnchor {
    /// Holds a provisional fence during initialization and retains it only on success.
    /// Failed or cancelled work keeps its own fence until its last worker releases it.
    pub async fn initialize_fenced<T, E, F: Future<Output = Result<T, E>>>(
        self,
        fence: impl Send + Sync + 'static,
        initialize: impl FnOnce(Self) -> F,
    ) -> Result<T, E> {
        // Both tuples drop the root before the fence, including on cancellation.
        // The fence never owns the root: committing cannot introduce an Arc cycle.
        let pending = (self, Arc::new(fence));
        let provisional = Self::new((pending.0.clone(), pending.1.clone()));
        let result = initialize(provisional).await;
        if result.is_ok() {
            pending.0._owner.retain_fence(Box::new(pending.1.clone()));
        }
        result
    }

    /// Retains a fence through cleanup of every clone, including pre-existing clones.
    pub fn fenced(owner: Self, fence: impl Send + Sync + 'static) -> Self {
        owner._owner.retain_fence(Box::new(fence));
        owner
    }

    /// Erases and retains one ownership value behind a cloneable lifetime token.
    pub fn new(owner: impl Send + Sync + 'static) -> Self {
        let owner = SharedOwner::<_, Box<dyn Send + Sync>>::new(owner);
        let release = OwnershipReleaseBarrier {
            released: Arc::clone(&owner.release.0),
        };
        Self {
            release,
            _owner: Arc::new(owner),
        }
    }

    /// Observes completion of the final owner's cleanup and retained fences.
    #[must_use]
    pub fn release_barrier(&self) -> OwnershipReleaseBarrier {
        OwnershipReleaseBarrier {
            released: Arc::clone(&self.release.released),
        }
    }
}

/// A non-owning release condition for an [`OwnershipAnchor`] clone family.
pub struct OwnershipReleaseBarrier {
    released: Arc<ReleaseState>,
}

struct ReleaseState {
    complete: AtomicBool,
    blocking_lock: Mutex<()>,
    blocking: Condvar,
    asynchronous: tokio::sync::Notify,
}

struct ReleaseSignal(Arc<ReleaseState>);

impl Drop for ReleaseSignal {
    fn drop(&mut self) {
        let guard = self
            .0
            .blocking_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.0.complete.store(true, Ordering::Release);
        drop(guard);
        self.0.blocking.notify_all();
        self.0.asynchronous.notify_waiters();
    }
}

impl OwnershipReleaseBarrier {
    /// Returns whether the final owner's cleanup and all fences have completed.
    #[must_use]
    pub fn is_released(&self) -> bool {
        self.released.complete.load(Ordering::Acquire)
    }

    /// Awaits final owner cleanup without blocking a thread or starting a timer.
    /// Dropping the last owner produces the event after its cleanup completes.
    pub async fn released(&self) {
        let changed = self.released.asynchronous.notified();
        tokio::pin!(changed);
        // Register before checking, including when cleanup runs concurrently.
        changed.as_mut().enable();
        if !self.is_released() {
            changed.await;
        }
    }

    /// Blocks until the final owner's cleanup and retained fences complete.
    pub fn wait(self) {
        self.wait_for(None);
    }

    /// Waits for owner release, refusing at the absolute deadline.
    /// A deadline bounds this blocking wait itself, including worker shutdown.
    pub fn wait_until(self, deadline: std::time::Instant) -> std::io::Result<()> {
        if self.wait_for(Some(deadline)) {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "ownership release deadline expired",
            ))
        }
    }

    fn wait_for(self, deadline: Option<std::time::Instant>) -> bool {
        let mut guard = self
            .released
            .blocking_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !self.is_released() {
            guard = match deadline {
                None => self
                    .released
                    .blocking
                    .wait(guard)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                Some(deadline) => {
                    let Some(remaining) =
                        deadline.checked_duration_since(std::time::Instant::now())
                    else {
                        return false;
                    };
                    self.released
                        .blocking
                        .wait_timeout(guard, remaining)
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .0
                }
            };
        }
        true
    }
}

#[cfg(test)]
#[cfg(loom)]
mod model;
