//! The Loom executor production persistence uses under `cfg(loom)`.
use super::*;

// Use the same model executor as snapshot workers: production tasks own their
// future independently of the caller, including after its join is dropped.
pub(super) fn spawn<F: Future + Send + 'static>(
    future: F,
) -> impl Future<Output = std::result::Result<F::Output, StreamError>>
where
    F::Output: Send + 'static,
{
    let (completed, received) = tokio::sync::oneshot::channel();
    crate::snapshot::model::spawn(async move {
        let _ = completed.send(future.await);
    });
    async move { received.await.map_err(|_| StreamError::Unavailable) }
}

pub(super) async fn spawn_blocking<T, F: FnOnce() -> T>(
    _medium: &'static str,
    _operation: &'static str,
    _key: PersistenceKey,
    work: F,
) -> std::result::Result<T, tokio::task::JoinError> {
    Ok(work())
}

// The same owned lease boundary, executed by the existing model worker.
pub(super) fn submit_file<C, T, E, A, F>(
    medium: &'static str,
    kind: &'static str,
    key: PersistenceKey,
    acquire: A,
    operation: F,
) -> impl Future<Output = std::result::Result<std::result::Result<T, E>, tokio::task::JoinError>>
+ use<C, T, E, A, F>
where
    A: FnOnce() -> std::result::Result<(acyclic_native_runtime::FileLease, C), E>,
    F: FnOnce(&acyclic_native_runtime::FileLease, &mut C) -> std::result::Result<T, E>,
{
    spawn_blocking(medium, kind, key, move || {
        // Fields drop in order even when the operation unwinds, matching the
        // production owner: the lease retires before its original custody.
        let mut owned = acquire()?;
        operation(&owned.0, &mut owned.1)
    })
}
