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

impl Drop for OwnershipReleaseBarrier {
    fn drop(&mut self) {
        if self.owner.strong_count() == 0 {
            let (lock, released) = &*self.released;
            let _guard = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            released.notify_all();
        }
    }
}

// Field order is the boundary: final shared-owner cleanup precedes every fence.
// Lock order: reopen fence -> `fences`; retain never acquires a reopen lock.
// Final cleanup drops the fence list without locking it.
struct SharedOwner<Owner, Fence> {
    _owner: Owner,
    fences: crate::sync::Mutex<Vec<Fence>>,
}

impl<Owner, Fence> SharedOwner<Owner, Fence> {
    fn new(owner: Owner) -> Self {
        Self {
            _owner: owner,
            fences: crate::sync::Mutex::new(Vec::new()),
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
        let owner: Arc<dyn RetainFence> =
            Arc::new(SharedOwner::<_, Box<dyn Send + Sync>>::new(owner));
        Self {
            release: OwnershipReleaseBarrier {
                owner: Arc::downgrade(&owner),
                released: Arc::new((Mutex::new(()), Condvar::new())),
            },
            _owner: owner,
        }
    }

    /// Observes the exact boundary at which every clone has released the owner.
    #[must_use]
    pub fn release_barrier(&self) -> OwnershipReleaseBarrier {
        OwnershipReleaseBarrier {
            owner: self.release.owner.clone(),
            released: Arc::clone(&self.release.released),
        }
    }
}

/// A non-owning release condition for an [`OwnershipAnchor`] clone family.
pub struct OwnershipReleaseBarrier {
    owner: Weak<dyn RetainFence>,
    released: Arc<(Mutex<()>, Condvar)>,
}

impl OwnershipReleaseBarrier {
    /// Returns whether every ownership-anchor clone has released the owner.
    #[must_use]
    pub fn is_released(&self) -> bool {
        self.owner.strong_count() == 0
    }

    /// Blocks until every ownership-anchor clone has released the owner.
    pub fn wait(self) {
        let (lock, released) = &*self.released;
        let mut guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.owner.strong_count() != 0 {
            guard = released
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(all(test, acyclic_native_runtime_loom))]
mod model;
