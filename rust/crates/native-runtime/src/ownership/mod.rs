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

struct ReleaseState<T> {
    complete: crate::sync::Mutex<bool>,
    synchronous: crate::sync::Condvar,
    asynchronous: tokio::sync::Notify,
    value: T,
}

/// Publishes retirement when dropped. Place this field after the resources
/// whose destruction must precede completion.
pub struct OwnershipReleaseSignal<T = ()>(Arc<ReleaseState<T>>);

impl<T> OwnershipReleaseSignal<T> {
    /// Complete shared-state allocation, including Arc counters, for callers
    /// that admit native control storage before construction.
    #[must_use]
    pub const fn allocation_bytes() -> usize {
        crate::arc_allocation_bytes::<ReleaseState<T>>()
    }

    /// Creates one retirement condition and its retained state. The caller
    /// supplies any allocation guard as part of the state.
    #[must_use]
    pub fn new(value: T) -> Self {
        Self(Arc::new(ReleaseState {
            complete: crate::sync::Mutex::new(false),
            synchronous: crate::sync::Condvar::new(),
            asynchronous: tokio::sync::Notify::new(),
            value,
        }))
    }

    /// State that remains available to observers after retirement.
    #[must_use]
    pub fn state(&self) -> &T {
        &self.0.value
    }

    /// Observes this exact retirement condition without retaining its owner.
    #[must_use]
    pub fn release_barrier(&self) -> OwnershipReleaseBarrier<T> {
        OwnershipReleaseBarrier {
            released: Arc::clone(&self.0),
        }
    }
}

impl<T> Drop for OwnershipReleaseSignal<T> {
    fn drop(&mut self) {
        {
            let mut complete = self
                .0
                .complete
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *complete = true;
        }
        self.0.synchronous.notify_all();
        self.0.asynchronous.notify_waiters();
    }
}

// Field order is the boundary: final shared-owner cleanup precedes every fence.
// Lock order: reopen fence -> `fences`; retain never acquires a reopen lock.
// Final cleanup drops the fence list without locking it.
struct SharedOwner<Owner, Fence> {
    _owner: Owner,
    fences: crate::sync::Mutex<Vec<Fence>>,
    // Last: observers learn completion only after payload and fences retire.
    released: OwnershipReleaseSignal,
}

impl<Owner, Fence> SharedOwner<Owner, Fence> {
    fn new(owner: Owner) -> Self {
        Self {
            _owner: owner,
            fences: crate::sync::Mutex::new(Vec::new()),
            released: OwnershipReleaseSignal::new(()),
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
            released: Arc::clone(&owner.released.0),
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
pub struct OwnershipReleaseBarrier<T = ()> {
    released: Arc<ReleaseState<T>>,
}

impl<T> OwnershipReleaseBarrier<T> {
    /// State retained through the final owner's retirement.
    #[must_use]
    pub fn state(&self) -> &T {
        &self.released.value
    }

    /// Returns whether every ownership-anchor clone has released the owner.
    #[must_use]
    pub fn is_released(&self) -> bool {
        *self
            .released
            .complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Blocks until final payload and fence retirement.
    pub fn wait(&self) {
        self.wait_for(None);
    }

    /// Waits for actual retirement, refusing at the original absolute deadline.
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

    fn wait_for(&self, deadline: Option<std::time::Instant>) -> bool {
        let mut complete = self
            .released
            .complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*complete {
            complete = match deadline {
                None => self
                    .released
                    .synchronous
                    .wait(complete)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                Some(deadline) => {
                    let Some(remaining) =
                        deadline.checked_duration_since(std::time::Instant::now())
                    else {
                        return false;
                    };
                    self.released
                        .synchronous
                        .wait_timeout(complete, remaining)
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .0
                }
            };
        }
        true
    }

    /// Awaits final payload and fence retirement without blocking an executor.
    /// Dropping this future removes its notification registration.
    pub async fn released(self) {
        loop {
            let changed = self.released.asynchronous.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_released() {
                return;
            }
            changed.await;
        }
    }
}

#[cfg(test)]
#[cfg(loom)]
mod model;
