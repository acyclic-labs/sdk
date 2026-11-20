use crate::sync::atomic::{AtomicU64, Ordering};
use acyclic_harness_core::sync::owned::{Arc, Mutex, OwnedMutexGuard, lock_owned};

/// A permit to perform one refresh, held for the whole attempt.
pub type Permit<F> = OwnedMutexGuard<Option<F>>;

/// Sole credential lock. Never acquire another refresh lock while holding it.
/// The generation is sampled before queuing; failures belong to that attempt,
/// not to subsequent callers. A permit follows accepted physical work even if
/// its async observer is cancelled; dropping it publishes no attempt result.
pub struct Refresh<F> {
    generation: AtomicU64,
    result: Arc<Mutex<Option<F>>>,
}

impl<F: Copy> Refresh<F> {
    /// A refresh lock with no attempt in flight.
    pub fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            result: Arc::new(Mutex::new(None)),
        }
    }

    pub fn acquire(&self) -> impl Future<Output = Result<Permit<F>, F>> {
        let generation = self.generation.load(Ordering::Acquire);
        self.acquire_generation(generation)
    }

    async fn acquire_generation(&self, generation: u64) -> Result<Permit<F>, F> {
        let result = lock_owned(self.result.clone()).await;
        if self.generation.load(Ordering::Acquire) != generation
            && let Some(failure) = *result
        {
            return Err(failure);
        }
        Ok(result)
    }

    /// Publishes FAILURE as this attempt's result and admits the next attempt.
    pub fn complete(&self, mut result: Permit<F>, failure: Option<F>) {
        *result = failure;
        self.generation.fetch_add(1, Ordering::Release);
    }
}
