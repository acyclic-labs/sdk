//! Never construct a child effect when delivery has already settled its request.
use crate::Result;
use std::future::Future;

pub(crate) async fn after_delivery<T, F: Future<Output = Result<Option<T>>>>(
    delivery: impl Future<Output = Result<Option<T>>>,
    complete: impl FnOnce() -> F,
) -> Result<Option<T>> {
    if let Some(answer) = delivery.await? {
        return Ok(Some(answer));
    }
    complete().await
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
