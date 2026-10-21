//! The one owned-task API for production callers.
//!
//! Callers name a single `spawn`/`spawn_blocking` regardless of build
//! features. A `test-support` build routes these through the seeded
//! simulation scheduler, which itself falls back to Tokio when no simulation
//! is installed; every other build submits directly to Tokio. Both report a
//! cancelled task as `ErrorKind::Interrupted`.
#[cfg(feature = "test-support")]
pub use crate::testing::{AbortHandle, TaskHandle, spawn, spawn_blocking};
#[cfg(not(feature = "test-support"))]
pub use native::{AbortHandle, TaskHandle, spawn, spawn_blocking};

/// Maps a Tokio join failure onto the shared task error contract.
pub(crate) fn join_error(cause: tokio::task::JoinError) -> std::io::Error {
    if cause.is_cancelled() {
        std::io::Error::new(std::io::ErrorKind::Interrupted, cause)
    } else {
        std::io::Error::other(cause)
    }
}

#[cfg(not(feature = "test-support"))]
mod native {
    use std::{
        future::Future,
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    /// Owned task handle. Dropping detaches; aborting cancels the future.
    pub struct TaskHandle<T>(tokio::task::JoinHandle<T>);

    /// Cancellation authority retained independently of the task's result.
    #[derive(Clone)]
    pub struct AbortHandle(tokio::task::AbortHandle);

    impl AbortHandle {
        /// Requests cancellation of the original owned future.
        pub fn abort(&self) {
            self.0.abort();
        }
    }

    impl<T> TaskHandle<T> {
        /// Cancels the task even if its caller no longer awaits it.
        pub fn abort(&self) {
            self.0.abort();
        }

        /// Retains cancellation authority without retaining the result.
        pub fn abort_handle(&self) -> AbortHandle {
            AbortHandle(self.0.abort_handle())
        }
    }

    impl<T> Future for TaskHandle<T> {
        type Output = io::Result<T>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.get_mut().0)
                .poll(cx)
                .map(|result| result.map_err(super::join_error))
        }
    }

    /// Submits an owned future to the current Tokio runtime.
    pub fn spawn<F>(future: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        TaskHandle(tokio::spawn(future))
    }

    /// Runs a blocking closure on Tokio's blocking pool, retaining ownership
    /// after the caller drops the returned future.
    pub fn spawn_blocking<F, T>(work: F) -> impl Future<Output = io::Result<T>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let task = tokio::task::spawn_blocking(work);
        async move { task.await.map_err(super::join_error) }
    }
}

#[cfg(all(test, not(acyclic_native_runtime_loom)))]
mod tests {
    #[cfg(feature = "test-support")]
    use std::time::Duration;

    // Outside a simulation, blocking work must stay visible to a paused Tokio
    // clock: virtual time may not auto-advance past an unfinished closure.
    // Negative control: routing this path through the native worker service
    // (invisible to Tokio) lets the hour-long virtual timeout elapse at once.
    #[test]
    #[cfg(feature = "test-support")]
    fn unsimulated_blocking_work_holds_a_paused_clock() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            release.send(()).unwrap();
        });
        let finished = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(3600),
                super::spawn_blocking(move || wait.recv().unwrap()),
            )
            .await
        });
        releaser.join().unwrap();
        assert!(matches!(finished, Ok(Ok(()))), "{finished:?}");
    }

    #[test]
    fn an_aborted_task_reports_interrupted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let error = runtime.block_on(async {
            let task = super::spawn(std::future::pending::<()>());
            task.abort();
            task.await.unwrap_err()
        });
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    }
}
