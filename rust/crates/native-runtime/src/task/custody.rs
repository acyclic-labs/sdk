//! Cancellation policy over the original submitted task and completion.
use super::{AbortHandle, TaskHandle};
use futures_util::{FutureExt as _, future::Shared};
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Submit one scoped future through the same native or deterministic executor
/// as [`super::spawn`]. Dropping its owner requests cancellation; explicitly
/// abort, await and drop the result to observe the future and output released.
pub fn spawn_owned<F>(future: F) -> OwnedTask<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    super::spawn(future).into()
}

/// Abort-on-drop ownership of one original task. Cancellation is cooperative:
/// it cannot interrupt a blocking poll, and drop is not physical completion.
/// Successful joining transfers the original output to the caller.
pub struct OwnedTask<T>(TaskHandle<T>);

impl<T> From<TaskHandle<T>> for OwnedTask<T> {
    fn from(task: TaskHandle<T>) -> Self {
        Self(task)
    }
}

impl<T> OwnedTask<T> {
    /// Requests cancellation without surrendering the completion observer.
    pub fn abort(&self) {
        self.0.abort();
    }

    /// Retains cancellation authority independently of the scoped owner.
    pub fn abort_handle(&self) -> AbortHandle {
        self.0.abort_handle()
    }
}

impl<T> Future for OwnedTask<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context)
    }
}

impl<T> Drop for OwnedTask<T> {
    fn drop(&mut self) {
        self.abort();
    }
}

// Preserve the original typed join error without formatting its panic payload.
enum JoinFailure {
    Native(tokio::task::JoinError),
    Panicked {
        _payload: std::sync::Mutex<Box<dyn std::any::Any + Send>>,
    },
    #[cfg(feature = "test-support")]
    Cancelled(tokio::sync::oneshot::error::RecvError),
}

impl JoinFailure {
    fn cancelled(&self) -> bool {
        match self {
            Self::Native(error) => error.is_cancelled(),
            Self::Panicked { .. } => false,
            #[cfg(feature = "test-support")]
            Self::Cancelled(_) => true,
        }
    }

    fn into_error(self) -> io::Error {
        let kind = if self.cancelled() {
            io::ErrorKind::Interrupted
        } else {
            io::ErrorKind::Other
        };
        io::Error::new(kind, self)
    }
}

impl fmt::Display for JoinFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(if self.cancelled() {
            "TaskCancelled"
        } else {
            "TaskPanicked"
        })
    }
}

impl fmt::Debug for JoinFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for JoinFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::Panicked { .. } => None,
            #[cfg(feature = "test-support")]
            Self::Cancelled(error) => Some(error),
        }
    }
}

pub(super) fn join_error(error: tokio::task::JoinError) -> io::Error {
    JoinFailure::Native(error).into_error()
}

pub(crate) fn panic_error(payload: Box<dyn std::any::Any + Send>) -> io::Error {
    JoinFailure::Panicked {
        _payload: std::sync::Mutex::new(payload),
    }
    .into_error()
}

#[cfg(feature = "test-support")]
pub(crate) fn cancelled_error(error: tokio::sync::oneshot::error::RecvError) -> io::Error {
    JoinFailure::Cancelled(error).into_error()
}

type CompletionFuture = Pin<Box<dyn Future<Output = Result<(), Arc<io::Error>>> + Send>>;

/// Cloneable observation of one independently owned task's original terminal.
/// Dropping observers never aborts the task. Remaining observers share its exact
/// operation error; joining failures have closed diagnostics and typed causes.
#[derive(Clone)]
pub struct TaskCompletion(Shared<CompletionFuture>);

impl TaskCompletion {
    /// Retains a submitted task without acquiring cancellation authority.
    pub fn new(task: TaskHandle<io::Result<()>>) -> Self {
        Self(
            async move {
                task.await
                    .and_then(std::convert::identity)
                    .map_err(Arc::new)
            }
            .boxed()
            .shared(),
        )
    }

    /// Observes the same terminal even after another waiter is cancelled.
    pub async fn wait(&self) -> io::Result<()> {
        self.0
            .clone()
            .await
            .map_err(|error| io::Error::new(error.kind(), error))
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
