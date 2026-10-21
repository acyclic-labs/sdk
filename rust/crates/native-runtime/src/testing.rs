//! Explicit, scoped barriers at real owned-task and native-job boundaries.
use std::{
    future::Future,
    sync::{Arc, PoisonError},
    task::{Poll, Waker},
};
use tokio::sync::{mpsc, oneshot};

use crate::sync::Mutex;
mod entropy;
pub use entropy::fill_seeded;
mod clock;
pub use clock::{CapturedClock, unix_millis};
mod executor;
pub use executor::{AbortHandle, TaskHandle};
mod durability;
pub use durability::{
    DurableFault, DurableWrite, InjectedStorageFault, durable_operation, durable_write,
};
pub(crate) mod host;
pub(crate) use host::Pool as HostPool;
pub use host::blocking_on;
mod process;
pub use process::{ProcessCapture, capture_process};
mod metadata;
pub use metadata::{NativeMetadataInput, NativeTimes, capture_native_times};
#[cfg(test)]
#[cfg(acyclic_native_runtime_loom)]
mod model;
#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod queue;
#[cfg(test)]
#[cfg(acyclic_native_runtime_loom)]
mod queue_model;

tokio::task_local! { static CURRENT: Option<Context>; }
tokio::task_local! { static BLOCKING_CONTEXT: Option<Context>; }
#[cfg(test)]
thread_local! {
    static FORGET_HOST_CONTEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

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
    native_active: usize,
    idle: Option<Waker>,
    cancelled: bool,
    trace: Vec<String>,
    ready_reads: usize,
    // Read only through a seeded schedule; see `Schedule::is_seeded`.
    wall: clock::Wall,
    writes: durability::Writes,
    process: Option<Arc<process::Provider>>,
    metadata: Option<Arc<metadata::Provider>>,
}

struct Inner {
    events: mpsc::UnboundedSender<Boundary>,
    // Never nested with the entropy lock, or held across callbacks/awaits/wakes.
    state: Mutex<State>,
    entropy: Option<Arc<entropy::Source>>,
    executor: Option<Arc<executor::Executor>>,
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
pub struct Schedule(Arc<Inner>, Option<Arc<Controller>>);

impl Clone for Schedule {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

// Accepted work retains its completion state, but does not own the controller
// that drives it. Otherwise executor -> future -> Context -> Schedule cycles.
struct Controller(Option<Arc<executor::Executor>>);

impl Drop for Controller {
    fn drop(&mut self) {
        if let Some(executor) = &self.0 {
            executor.shutdown();
        }
    }
}

/// An original submission's cancellation-independent completion context.
#[derive(Clone)]
pub(crate) struct Context {
    schedule: Schedule,
    owned: bool,
    // Never nested with the schedule-state lock; cloned before sends/callbacks.
    detail: Arc<Mutex<Arc<str>>>,
    host: Option<Arc<host::Host>>,
}

pub(crate) struct Active(pub(crate) Context, bool);
impl Drop for Active {
    fn drop(&mut self) {
        let idle = {
            let mut state = self.0.schedule.0.state();
            // Only `Context::accept` constructs an `Active`, after counting it.
            state.active -= 1;
            state.native_active -= usize::from(self.1);
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
        Self::with_seed(None, true)
    }

    /// Creates a simulation scope with reproducible entropy and owned-task polling.
    /// Neither changes ordinary execution outside the scope and its descendants.
    pub fn seeded(seed: u64) -> (Self, mpsc::UnboundedReceiver<Boundary>) {
        Self::with_seed(Some(seed), true)
    }

    /// Negative control: erase scoped entropy while preserving the exact same
    /// executor, clock, owned work and physical I/O completion boundaries.
    pub fn without_entropy(seed: u64) -> (Self, mpsc::UnboundedReceiver<Boundary>) {
        Self::with_seed(Some(seed), false)
    }

    fn with_seed(seed: Option<u64>, entropy: bool) -> (Self, mpsc::UnboundedReceiver<Boundary>) {
        let (events, receiver) = mpsc::unbounded_channel();
        let executor = seed.map(executor::Executor::new);
        let controller = Arc::new(Controller(executor.clone()));
        (
            Self(
                Arc::new(Inner {
                    events,
                    state: Mutex::new(State::default()),
                    entropy: seed
                        .filter(|_| entropy)
                        .map(|seed| Arc::new(entropy::Source::new(seed))),
                    executor,
                }),
                Some(controller),
            ),
            receiver,
        )
    }

    /// Instruments only this explicitly scoped future and its owned descendants.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        // Re-entering the same scope retains its root and ownership context.
        // A nested caller's waker already routes through that root; registering
        // it as a second driver would make the wake route point back to itself.
        if CURRENT
            .try_with(|context| {
                context
                    .as_ref()
                    .is_some_and(|context| Arc::ptr_eq(&context.schedule.0, &self.0))
            })
            .unwrap_or(false)
        {
            return future.await;
        }
        if self.is_seeded() {
            self.0.state().wall.start();
        }
        let future = CURRENT.scope(
            Some(Context {
                schedule: Self(self.0.clone(), None),
                owned: false,
                detail: Arc::new(Mutex::new(Arc::from(""))),
                host: None,
            }),
            future,
        );
        match &self.0.executor {
            Some(executor) => executor.run(future).await,
            None => future.await,
        }
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
            if let Some(executor) = &self.0.executor {
                executor.poll_owned(cx);
            }
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

    /// Accepted native/blocking owners, including physical work after caller cancellation.
    /// Task owners waiting on streams are excluded; they cannot hide a missing follow.
    pub fn pending_native_work(&self) -> usize {
        self.0.state().native_active
    }

    /// Original submissions and their completion trace, including cancellation.
    pub fn trace(&self) -> Vec<String> {
        self.0.state().trace.clone()
    }

    /// Exact seeded task-poll trace, including detached completions.
    pub fn polls(&self) -> Vec<(u64, &'static str)> {
        self.0
            .executor
            .as_ref()
            .map_or_else(Vec::new, |executor| executor.trace())
    }
}

impl Context {
    pub(crate) fn current() -> Option<Self> {
        CURRENT.try_with(Clone::clone).ok().flatten()
    }

    fn simulation() -> Option<Self> {
        Self::current().or_else(|| BLOCKING_CONTEXT.try_with(Clone::clone).ok().flatten())
    }

    pub(crate) fn accept(&self, kind: &'static str) -> Active {
        let native = matches!(kind, "native" | "blocking");
        {
            let mut state = self.schedule.0.state();
            assert!(
                !state.cancelled || self.owned,
                "new dispatch after cancellation: {kind}"
            );
            // Cancellation and registration share the same lock. Once cancel
            // returns, drain cannot miss an already accepted submission.
            state.active += 1;
            state.native_active += usize::from(native);
            state.trace.push(format!(
                "{kind}:{}",
                if self.owned { "completion" } else { "dispatch" }
            ));
        }
        Active(
            Self {
                schedule: self.schedule.clone(),
                owned: true,
                detail: Arc::new(Mutex::new(lock(&self.detail).clone())),
                host: self.host.clone(),
            },
            native,
        )
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

/// Runs the original owned future with deterministic submission/terminal gates.
pub fn spawn<F>(future: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let Some(context) = Context::current() else {
        return TaskHandle::native(tokio::spawn(future));
    };
    let active = context.accept("task");
    let owned = active.0.clone();
    spawn_owned(
        &context,
        CURRENT.scope(Some(owned.clone()), async move {
            let _active = active;
            owned.pause("task:submitted").await;
            let result = future.await;
            owned.pause("task:completed").await;
            result
        }),
    )
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
        BlockingOwner::Controlled(spawn_owned(
            &context,
            CURRENT.scope(Some(owned.clone()), async move {
                let _active = active;
                owned.pause("blocking:submitted").await;
                let host = if owned.schedule.0.executor.is_some() {
                    Some(host::Host::for_workers(crate::workers(
                        &crate::BLOCKING_WORKERS,
                        false,
                    )?))
                } else {
                    None
                };
                let completion = crate::run_blocking_io(
                    move || {
                        let _inline = crate::InlineBlocking::enter();
                        work()
                    },
                    None,
                );
                let result = host_work(completion, host).await;
                owned.pause("blocking:completed").await;
                result
            }),
        ))
    } else if crate::inline_blocking_allowed() {
        // These closures may use Tokio blocking locks, so they need a real
        // worker outside the driver's runtime. Reuse the native service instead
        // of creating a private Tokio blocking pool for every replay driver.
        BlockingOwner::Shared(tokio::spawn(async move {
            host_work(crate::run_blocking_io(work, None), None).await
        }))
    } else {
        // Without a simulation this is the production path: Tokio's blocking
        // pool, which a paused Tokio clock observes as outstanding work, so
        // virtual time cannot advance past an unfinished closure.
        BlockingOwner::Worker(tokio::task::spawn_blocking(work))
    };
    async move {
        match owner {
            BlockingOwner::Controlled(worker) => worker.await?,
            BlockingOwner::Shared(shared) => shared.await.map_err(crate::task::join_error)?,
            BlockingOwner::Worker(worker) => worker.await.map_err(crate::task::join_error),
        }
    }
}

/// The one task that owns a blocking closure's result.
enum BlockingOwner<T> {
    Controlled(TaskHandle<std::io::Result<T>>),
    Shared(tokio::task::JoinHandle<std::io::Result<T>>),
    Worker(tokio::task::JoinHandle<T>),
}

async fn host_work<T>(
    completion: std::io::Result<crate::NativeCompletion<T>>,
    host: Option<Arc<host::Host>>,
) -> std::io::Result<T> {
    let mut completion = completion?;
    if let Some(mut job) = completion.pending.take() {
        let workers = crate::workers(&crate::BLOCKING_WORKERS, false)?;
        host::Work::bind(&mut job, Context::current(), host.clone());
        if let Some(host) = host {
            host.drive(queued(job, workers)).await?;
        } else {
            queued(job, workers).await?;
        }
    }
    completion.await
}

pub(crate) fn submit(
    context: &Context,
    mut job: crate::NativeJob,
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
    drop(spawn_owned(
        context,
        CURRENT.scope(Some(owned.clone()), async move {
            let _active = active;
            owned.pause("native:submitted").await;
            let result = match crate::workers(pool, job.meter().is_some()) {
                Ok(workers) => {
                    let host = owned
                        .schedule
                        .0
                        .executor
                        .as_ref()
                        .map(|_| host::Host::for_workers(workers));
                    host::Work::bind(&mut job, Some(owned.clone()), host.clone());
                    match host {
                        Some(host) => host.drive(queued(job, workers)).await,
                        None => queued(job, workers).await,
                    }
                }
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
        }),
    ));
    waiting
}

fn spawn_owned<F>(context: &Context, future: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match &context.schedule.0.executor {
        Some(executor) => executor.spawn(future),
        None => TaskHandle::native(tokio::spawn(future)),
    }
}

// Retain only the simulated external sources on real host workers. CURRENT
// stays unset there: a blocking operation must not dispatch async barriers
// back into the executor that is waiting for its physical terminal.
pub(crate) fn inherit_host<F, T>(work: F) -> impl FnOnce() -> T + Send + 'static
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let context = Context::simulation();
    #[cfg(test)]
    let context = if FORGET_HOST_CONTEXT.with(|fault| fault.replace(false)) {
        None
    } else {
        context
    };
    // Carry simulation sources without entering Tokio around user blocking
    // work. Clock reads enter their captured runtime only for the read itself.
    move || {
        let context = context.map(|mut context| {
            // Inline work retains its physical host; queued work borrows the
            // receiving worker's host, never the submitting worker's permit.
            context.host = host::Host::current();
            let _ = BLOCKING_CONTEXT.try_with(|physical| {
                if let Some(physical) = physical
                    && Arc::ptr_eq(&context.schedule.0, &physical.schedule.0)
                {
                    context.owned |= physical.owned;
                }
            });
            context
        });
        BLOCKING_CONTEXT.sync_scope(context, work)
    }
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
