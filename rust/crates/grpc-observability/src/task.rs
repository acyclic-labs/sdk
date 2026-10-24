//! Spawn boundaries preserve the caller's scoped tracing dispatcher.
use std::future::Future;
use tracing::instrument::WithSubscriber as _;

/// Spawn a future with the caller's dispatcher on every poll.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.with_current_subscriber())
}

/// Admit a future to a join set with the caller's dispatcher.
pub fn join_set<F>(set: &mut tokio::task::JoinSet<F::Output>, future: F) -> tokio::task::AbortHandle
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    set.spawn(future.with_current_subscriber())
}

/// Capture the dispatcher at admission, rather than on the worker thread.
pub fn blocking<F, T>(work: F) -> impl FnOnce() -> T + Send
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    move || tracing::dispatcher::with_default(&dispatch, work)
}

/// Spawn blocking work on the current runtime.
pub fn spawn_blocking<F, T>(work: F) -> tokio::task::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(blocking(work))
}

/// Spawn blocking work on an explicitly retained runtime.
pub fn runtime_blocking<F, T>(
    runtime: &tokio::runtime::Handle,
    work: F,
) -> tokio::task::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    runtime.spawn_blocking(blocking(work))
}

/// Start a thread with its caller's scoped dispatcher.
pub fn thread<F, T>(work: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(blocking(work))
}

/// Start a named/configured thread with its caller's scoped dispatcher.
pub fn named_thread<F, T>(
    builder: std::thread::Builder,
    work: F,
) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    builder.spawn(blocking(work))
}

/// Start a borrowing scope worker with its caller's scoped dispatcher.
pub fn scoped_thread<'scope, 'env, F, T>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    work: F,
) -> std::thread::ScopedJoinHandle<'scope, T>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    scope.spawn(blocking(work))
}

#[cfg(test)]
mod tests;
