use crate::sync::{
    AsyncMutex,
    atomic::{AtomicU64, Ordering},
};

/// Sole credential lock. Never acquire another refresh lock while holding it.
/// The generation is sampled before queuing; failures belong to that attempt,
/// not to subsequent callers. Cancellation drops ownership without publication.
pub(crate) struct Refresh<F> {
    generation: AtomicU64,
    result: AsyncMutex<Option<F>>,
}

impl<F> Refresh<F> {
    pub(crate) fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            result: AsyncMutex::new(None),
        }
    }
}

pub(crate) fn serialized<T, F: Copy>(
    state: &Refresh<F>,
    operation: impl std::future::Future<Output = (T, Option<F>)>,
) -> impl std::future::Future<Output = Result<T, F>> {
    let generation = state.generation.load(Ordering::Acquire);
    execute(state, generation, operation)
}

async fn execute<T, F: Copy>(
    state: &Refresh<F>,
    generation: u64,
    operation: impl std::future::Future<Output = (T, Option<F>)>,
) -> Result<T, F> {
    let mut result = state.result.lock().await;
    if state.generation.load(Ordering::Acquire) != generation {
        if let Some(failure) = *result {
            return Err(failure);
        }
    }
    let (value, failure) = operation.await;
    *result = failure;
    state.generation.fetch_add(1, Ordering::Release);
    Ok(value)
}
