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

/// Runtime for a single coordination owner. Durable storage uses its own shared,
/// fixed service through [`spawn_persistence`]. Interactive blocking reads may
/// occupy the incidental Tokio worker without starving storage producers.
pub fn coordination_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
}

/// Submits durable storage work to the shared, bounded persistence service.
///
/// Submission is eager: a replay reader may await records produced by the
/// closure before it awaits this result. Once admitted, the service retains
/// the closure through physical completion if the caller disappears. Closures
/// must not wait for work submitted to this same service.
pub fn spawn_persistence<F, T>(
    work: F,
) -> impl std::future::Future<Output = std::io::Result<T>> + Send
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let completion = crate::task_completion(
        move || Ok(work()),
        None,
        None,
        None,
        &crate::workers::PERSISTENCE_WORKERS,
    );
    let task = spawn(async move { completion?.await });
    async move { task.await? }
}

/// The caller's task submission context, retained across a native worker turn.
/// Entering it submits through the ordinary owned-task API and immediately
/// leaves it; native IO callbacks still run outside the asynchronous runtime.
pub struct TaskSpawner {
    runtime: tokio::runtime::Handle,
    #[cfg(feature = "test-support")]
    context: Option<crate::testing::Context>,
}

impl TaskSpawner {
    /// Captures the current runtime and original simulation task, if any.
    #[must_use]
    pub fn current() -> Self {
        Self {
            runtime: tokio::runtime::Handle::current(),
            #[cfg(feature = "test-support")]
            context: crate::testing::Context::current(),
        }
    }

    /// Transfers a native owner's future directly to the original task scope.
    pub fn spawn<F>(&self, future: F) -> TaskHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let _runtime = self.runtime.enter();
        #[cfg(feature = "test-support")]
        {
            crate::testing::spawn_in_context(self.context.clone(), future)
        }
        #[cfg(not(feature = "test-support"))]
        {
            spawn(future)
        }
    }
}

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

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    #[cfg(feature = "test-support")]
    use std::time::Duration;

    #[test]
    fn coordination_blocking_pool_stays_flat_with_more_tasks() {
        fn identities(runtime: &tokio::runtime::Runtime, tasks: usize, starters: usize) -> usize {
            let (send, receive) = std::sync::mpsc::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let wait = std::sync::Arc::new(std::sync::Mutex::new(wait));
            let mut jobs = Vec::new();
            // The first worker is physically occupied before further jobs are
            // submitted. An expanding pool has to start another worker; a
            // fixed owner queues them without allocating another thread.
            for index in 0..tasks {
                let send = send.clone();
                let wait = std::sync::Arc::clone(&wait);
                jobs.push(runtime.spawn_blocking(move || {
                    let identity = std::thread::current().id();
                    send.send(identity).unwrap();
                    wait.lock().unwrap().recv().unwrap();
                    identity
                }));
                if index == 0 {
                    receive.recv().unwrap();
                }
            }
            // The expanding-pool control must physically start a second
            // worker before the first is released, rather than depending on
            // which worker wins the runnable queue after release.
            for _ in 1..starters {
                receive.recv().unwrap();
            }
            for _ in 0..tasks {
                release.send(()).unwrap();
            }
            runtime.block_on(async {
                let mut identities = std::collections::HashSet::new();
                for job in jobs {
                    identities.insert(job.await.unwrap());
                }
                identities.len()
            })
        }
        let runtime = super::coordination_runtime().unwrap();
        assert_eq!(identities(&runtime, 4, 1), 1);
        assert_eq!(identities(&runtime, 8, 1), 1);
        // Actual previous owner construction: Tokio's default blocking pool.
        let expanding = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(identities(&expanding, 8, 2) > 1);
    }

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
    fn persistence_retains_accepted_work_after_caller_drop() {
        let runtime = super::coordination_runtime().unwrap();
        runtime.block_on(async {
            let (started, starting) = tokio::sync::oneshot::channel();
            let (release, held) = std::sync::mpsc::channel::<()>();
            let (finished, mut finishing) = tokio::sync::oneshot::channel();
            let submitted = super::spawn_persistence(move || {
                let _ = started.send(());
                let error = held.recv().unwrap_err();
                let result = std::io::Error::new(std::io::ErrorKind::Interrupted, error);
                let _ = finished.send(result.kind());
            });
            // Submission is driven by its owner, even without polling this
            // result; caller cancellation cannot abandon accepted work.
            drop(submitted);
            starting.await.unwrap();
            assert!(matches!(
                finishing.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            drop(release);
            assert_eq!(finishing.await.unwrap(), std::io::ErrorKind::Interrupted);
        });
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
