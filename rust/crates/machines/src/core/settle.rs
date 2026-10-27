//! Bounded observation shared by asynchronous native resources. No mutation is retried.
use crate::{IdempotencyKey, ProviderError};
use std::time::Duration;

/// Observe until a provider reports a terminal native result. A missing/new native
/// object may be translated to `None` by its wire adapter. The deadline bounds the
/// observation itself as well as gaps between reads, and never permits redispatch.
pub async fn settle<T, F, O>(
    key: IdempotencyKey,
    interval: Duration,
    timeout: Duration,
    mut observe: F,
) -> Result<T, ProviderError>
where
    F: FnMut() -> O + Send,
    O: std::future::Future<Output = Result<Option<T>, ProviderError>> + Send,
    T: Send,
{
    if interval.is_zero() || timeout.is_zero() {
        return Err(ProviderError::Invalid("Machines observation admission failed; recovery: configure positive poll_interval and ready_timeout".into()));
    }
    let deadline = tokio::time::Instant::now().checked_add(timeout).ok_or_else(|| ProviderError::Invalid("Machines observation deadline overflow; recovery: configure a shorter ready_timeout".into()))?;
    tokio::time::timeout_at(deadline, async {
        loop {
            if let Some(value) = observe().await? {
                return Ok(value);
            }
            tokio::time::sleep(interval).await;
        }
    })
    .await
    .map_err(|_| ProviderError::Indeterminate(key))?
}
