//! Explicit, scoped barriers at real owned-task and native-job boundaries.
use std::{
    future::Future,
    sync::{Arc, PoisonError},
    task::{Poll, Waker},
};
use tokio::sync::{mpsc, oneshot};

use crate::sync::Mutex;
#[cfg(all(test, acyclic_native_runtime_loom))]
mod model;
#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod queue;
#[cfg(all(test, acyclic_native_runtime_loom))]
mod queue_model;

tokio::task_local! { static CURRENT: Option<Context>; }

/// A submitted operation paused before execution or acknowledgement.
pub struct Boundary {
    /// Stable phase, independent of executor poll counts.
    pub name: &'static str,
    /// Owning operation, including its stream's tracing span.
    pub detail: Arc<str>,
    release: oneshot::Sender<()>,
}

impl Boundary {
    /// Allows the original owned operation to continue.
    pub fn release(self) {
        self.release.send(()).ok();
    }
}

#[derive(Default)]
struct State {
    active: usize,
    idle: Option<Waker>,
    cancelled: bool,
    trace: Vec<String>,
    ready_reads: usize,
}

struct Inner {
    events: mpsc::UnboundedSender<Boundary>,
    // Sole lock. Never held across an await, user callback, send, or wake.
    state: Mutex<State>,
}

impl Inner {
    fn state(&self) -> impl std::ops::DerefMut<Target = State> + '_ {
        lock(&self.state)
    }
}

// A panicking test thread leaves the trace readable for the driver's report.
fn lock<T>(mutex: &Mutex<T>) -> impl std::ops::DerefMut<Target = T> + '_ {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One test execution; no process-global controller or background driver.
#[derive(Clone)]
pub struct Schedule(Arc<Inner>);

/// An original submission's cancellation-independent completion context.
#[derive(Clone)]
pub(crate) struct Context {
    schedule: Schedule,
    owned: bool,
    // Never nested with the schedule-state lock; cloned before sends/callbacks.
    detail: Arc<Mutex<Arc<str>>>,
}

pub(crate) struct Active(pub(crate) Context);
impl Drop for Active {
    fn drop(&mut self) {
        let idle = {
            let mut state = self.0.schedule.0.state();
            // Only `Context::accept` constructs an `Active`, after counting it.
            state.active -= 1;
            (state.active == 0).then(|| state.idle.take()).flatten()
        };
        if let Some(idle) = idle {
            idle.wake();
        }
    }
}

impl Schedule {
    /// Creates the push receiver used by a deterministic test driver.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Boundary>) {
        let (events, receiver) = mpsc::unbounded_channel();
        (
            Self(Arc::new(Inner {
                events,
                state: Mutex::new(State::default()),
            })),
            receiver,
        )
    }

    /// Instruments only this explicitly scoped future and its owned descendants.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        CURRENT
            .scope(
                Some(Context {
                    schedule: self.clone(),
                    owned: false,
                    detail: Arc::new(Mutex::new(Arc::from(""))),
                }),
                future,
            )
            .await
    }

    /// Marks cancellation before the caller's future is dropped.
    pub fn cancel(&self) {
        let mut state = self.0.state();
        state.cancelled = true;
        state.trace.push("CANCEL".into());
    }

    /// Waits for accepted operations to reach their physical terminal.
    pub async fn drained(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.0.state();
            if state.active == 0 {
                Poll::Ready(())
            } else {
                state.idle = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }

    /// Counts the actual ready read calls, without inventing suspension points.
    pub fn ready_reads(&self) -> usize {
        self.0.state().ready_reads
    }

    /// Original submissions and their completion trace, including cancellation.
    pub fn trace(&self) -> Vec<String> {
        self.0.state().trace.clone()
    }
}

impl Context {
    pub(crate) fn current() -> Option<Self> {
        CURRENT.try_with(Clone::clone).ok().flatten()
    }

    pub(crate) fn accept(&self, kind: &'static str) -> Active {
        {
            let mut state = self.schedule.0.state();
            assert!(
                !state.cancelled || self.owned,
                "new dispatch after cancellation: {kind}"
            );
            // Cancellation and registration share the same lock. Once cancel
            // returns, drain cannot miss an already accepted submission.
            state.active += 1;
            state.trace.push(format!(
                "{kind}:{}",
                if self.owned { "completion" } else { "dispatch" }
            ));
        }
        Active(Self {
            schedule: self.schedule.clone(),
            owned: true,
            detail: Arc::new(Mutex::new(lock(&self.detail).clone())),
        })
    }

    async fn pause(&self, name: &'static str) {
        let (release, waiting) = oneshot::channel();
        let detail = lock(&self.detail).clone();
        // A dropped driver drops the boundary, and its release with it.
        self.schedule
            .0
            .events
            .send(Boundary {
                name,
                detail,
                release,
            })
            .ok();
        assert!(
            waiting.await.is_ok(),
            "driver dropped an accepted operation's barrier"
        );
    }
}

/// Pauses a real provider read before dispatch or after completion.
pub async fn boundary(name: &'static str) {
    if let Some(context) = Context::current() {
        context.pause(name).await;
    }
}

/// Names the original storage operation without exposing its payload.
pub fn describe(detail: impl FnOnce() -> String) {
    if let Some(context) = Context::current() {
        let detail = detail();
        *lock(&context.detail) = detail.into();
    }
}

/// Runs the same Tokio owned task with deterministic submission/terminal gates.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let Some(context) = Context::current() else {
        return tokio::spawn(future);
    };
    let active = context.accept("task");
    let owned = active.0.clone();
    tokio::spawn(CURRENT.scope(Some(owned.clone()), async move {
        let _active = active;
        owned.pause("task:submitted").await;
        let result = future.await;
        owned.pause("task:completed").await;
        result
    }))
}

/// Executes the real blocking closure, retaining ownership after caller drop.
pub fn spawn_blocking<F, T>(work: F) -> impl Future<Output = std::io::Result<T>> + Send
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let owner = if let Some(context) = Context::current() {
        let active = context.accept("blocking");
        let owned = active.0.clone();
        BlockingOwner::Worker(tokio::spawn(CURRENT.scope(
            Some(owned.clone()),
            async move {
                let _active = active;
                owned.pause("blocking:submitted").await;
                let result = CURRENT.sync_scope(None, || {
                    let _inline = crate::InlineBlocking::enter();
                    work()
                });
                owned.pause("blocking:completed").await;
                result
            },
        )))
    } else if crate::inline_blocking_allowed() {
        // These closures may use Tokio blocking locks, so they need a real
        // worker outside the driver's runtime. Reuse the native service instead
        // of creating a private Tokio blocking pool for every replay driver.
        BlockingOwner::Shared(tokio::spawn(async move {
            let mut completion = crate::run_blocking_io(work, None)?;
            if let Some(job) = completion.pending.take() {
                queued(job, crate::workers(&crate::BLOCKING_WORKERS, false)?).await?;
            }
            completion.await
        }))
    } else {
        BlockingOwner::Worker(tokio::task::spawn_blocking(work))
    };
    async move {
        match owner {
            BlockingOwner::Shared(shared) => shared.await.map_err(std::io::Error::other)?,
            BlockingOwner::Worker(worker) => worker.await.map_err(std::io::Error::other),
        }
    }
}

/// The one task that owns a blocking closure's result.
enum BlockingOwner<T> {
    Worker(tokio::task::JoinHandle<T>),
    Shared(tokio::task::JoinHandle<std::io::Result<T>>),
}

pub(crate) fn submit(
    context: &Context,
    job: crate::NativeJob,
    pool: &'static std::sync::LazyLock<std::io::Result<crate::NativeWorkers>>,
) -> oneshot::Receiver<std::io::Result<()>> {
    let active = context.accept("native");
    let owned = active.0.clone();
    let completion = match &job {
        crate::NativeJob::Read { completion, .. }
        | crate::NativeJob::Write { completion, .. }
        | crate::NativeJob::Task { completion, .. } => completion.clone(),
    };
    let (done, waiting) = oneshot::channel();
    tokio::spawn(CURRENT.scope(Some(owned.clone()), async move {
        let _active = active;
        owned.pause("native:submitted").await;
        let result = match crate::workers(pool, job.meter().is_some()) {
            Ok(workers) => queued(job, workers).await,
            Err(error) => Err(error),
        };
        if result.is_err()
            && let Some(completion) = completion
        {
            // The caller may already be gone; failure must retire its fence too.
            completion.complete();
        }
        owned.pause("native:completed").await;
        let _ = done.send(result);
    }));
    waiting
}

// Keep the original queue admission, capacity wake, worker handoff and physical
// terminal. Only the caller's acknowledgement is held at the surrounding gates.
async fn queued(job: crate::NativeJob, workers: &crate::NativeWorkers) -> std::io::Result<()> {
    let terminal = job.test_terminal();
    let mut pending = Some(job);
    let mut waiter = None;
    let result = std::future::poll_fn(|cx| {
        crate::workers::poll_submission_with(workers, &mut pending, &mut waiter, cx)
    })
    .await;
    crate::remove_capacity_waiter(workers, waiter);
    result?;
    std::future::poll_fn(|cx| terminal.test_poll_completion(cx.waker())).await;
    Ok(())
}

/// Observes actual blocking reads; an immediately ready read is one atomic poll.
pub async fn observe_read<F: Future>(read: F) -> F::Output {
    let Some(context) = Context::current() else {
        return read.await;
    };
    let mut read = std::pin::pin!(read);
    let first = std::future::poll_fn(|cx| std::task::Poll::Ready(read.as_mut().poll(cx))).await;
    match first {
        std::task::Poll::Ready(output) => {
            context.schedule.0.state().ready_reads += 1;
            output
        }
        std::task::Poll::Pending => {
            context.pause("stream:read:blocked").await;
            let output = read.await;
            context.pause("stream:read:completed").await;
            output
        }
    }
}

/// Runs the deliberately uninstrumented variant used by negative controls.
pub async fn untracked<F: Future>(future: F) -> F::Output {
    CURRENT.scope(None, future).await
}
