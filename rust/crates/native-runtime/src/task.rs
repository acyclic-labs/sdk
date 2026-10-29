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
/// fixed service through [`spawn_persistence`]. Its one blocking slot runs only
/// work that completes on its own, in submission order; an open stdin, whose
/// read may never return, is read on its own thread through [`crate::stdin`].
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
/// must not wait for work submitted to this same service: reentrant submission
/// returns [`std::io::ErrorKind::WouldBlock`] before entering its queue.
/// `medium`, `operation` and the opaque `key` identify the original producer in
/// wait reports. Never include credentials, user content or paths in the key.
pub fn spawn_persistence<F, T>(
    medium: &'static str,
    operation: &'static str,
    key: &dyn std::fmt::Display,
    work: F,
) -> impl std::future::Future<Output = std::io::Result<T>> + Send + use<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let waiting = crate::obs::waiting(
        "persistence-completion",
        format_args!("{medium}/{operation}/{key}"),
        std::future::pending::<()>(),
    );
    let progress = waiting.context().clone();
    let completion = if PERSISTENCE_JOB.with(|job| job.borrow().is_some()) {
        Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
    } else {
        progress.in_scope(|| {
            crate::task_completion(
                move || Ok(work()),
                None,
                None,
                None,
                &crate::workers::PERSISTENCE_WORKERS,
            )
        })
    };
    let task = completion.map(|completion| {
        spawn(async move {
            let _waiting = waiting;
            completion.await
        })
    });
    async move { task?.await? }
}

thread_local! {
    static PERSISTENCE_JOB: std::cell::RefCell<Option<crate::obs::OperationSpan>> = const { std::cell::RefCell::new(None) };
}

pub(crate) struct PersistenceTurn(Option<crate::obs::OperationSpan>);

impl PersistenceTurn {
    pub(crate) fn enter(progress: Option<crate::obs::OperationSpan>) -> Self {
        Self(PERSISTENCE_JOB.with(|job| job.replace(progress)))
    }
}

impl Drop for PersistenceTurn {
    fn drop(&mut self) {
        PERSISTENCE_JOB.with(|job| job.replace(self.0.take()));
    }
}

/// Marks a physical call in the original persistence job, without submitting work.
pub fn persistence_syscall<T>(call: impl FnOnce() -> T) -> T {
    struct Running;
    impl Drop for Running {
        fn drop(&mut self) {
            PERSISTENCE_JOB.with(|job| {
                if let Some(job) = job.borrow().as_ref() {
                    job.record("stage", 2_u64);
                }
            });
        }
    }
    PERSISTENCE_JOB.with(|job| {
        if let Some(job) = job.borrow().as_ref() {
            job.record("stage", 3_u64);
        }
    });
    let _running = Running;
    call()
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
    fn persistence_reports_the_original_job_and_refuses_same_service_dependencies() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let report = crate::obs::WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        let _dispatch = tracing::dispatcher::set_default(&dispatch);
        let runtime = super::coordination_runtime().unwrap();
        runtime.block_on(async {
            let (started, starting) = tokio::sync::oneshot::channel();
            let (release, held) = std::sync::mpsc::channel();
            let job = super::spawn_persistence("local", "write", &17, move || {
                super::persistence_syscall(|| {
                    started.send(()).unwrap();
                    held.recv().unwrap();
                });
                // The negative control attempts to wait on its own service.
                // Its body must never enter that service, even with free workers.
                let nested = super::spawn_persistence("local", "nested", &18, || {
                    panic!("reentrant persistence body ran")
                });
                let mut nested = std::pin::pin!(nested);
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(matches!(std::future::Future::poll(nested.as_mut(), &mut context),
                    std::task::Poll::Ready(Err(error)) if error.kind() == std::io::ErrorKind::WouldBlock));
            });
            starting.await.unwrap();
            let mut bytes = Vec::new();
            report.report(&mut bytes).unwrap();
            let observed = String::from_utf8(bytes).unwrap();
            assert!(observed.contains("local/write/17"), "{observed}");
            assert!(observed.contains("stage=syscall"), "{observed}");
            release.send(()).unwrap();
            job.await.unwrap();
            let mut bytes = Vec::new();
            report.report(&mut bytes).unwrap();
            assert!(bytes.is_empty(), "a terminal job remained live");
        });
    }

    #[test]
    fn persistence_worker_panic_terminates_its_original_wait() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let report = crate::obs::WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        let _dispatch = tracing::dispatcher::set_default(&dispatch);
        let runtime = super::coordination_runtime().unwrap();
        let error = runtime
            .block_on(super::spawn_persistence("local", "panic", &19, || {
                panic!("original persistence failure")
            }))
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        let mut bytes = Vec::new();
        report.report(&mut bytes).unwrap();
        assert!(bytes.is_empty(), "a failed producer retained its wait");
    }

    #[test]
    fn persistence_retains_accepted_work_after_caller_drop() {
        let runtime = super::coordination_runtime().unwrap();
        runtime.block_on(async {
            let (started, starting) = tokio::sync::oneshot::channel();
            let (release, held) = std::sync::mpsc::channel::<()>();
            let (finished, mut finishing) = tokio::sync::oneshot::channel();
            let submitted = super::spawn_persistence("test", "retained", &"held", move || {
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
