//! Single-threaded polling of the original owned futures, in seeded ready order.
use super::*;
#[cfg(acyclic_native_runtime_loom)]
use loom::sync::Condvar;
#[cfg(not(acyclic_native_runtime_loom))]
use std::sync::Condvar;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    pin::Pin,
    task::{Context as PollContext, Wake},
};

type Work = Pin<Box<dyn Future<Output = ()> + Send>>;

tokio::task_local! { static POLLING_EXECUTOR: Arc<Executor>; }

#[cfg(test)]
#[cfg(acyclic_native_runtime_loom)]
mod models;
#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod tests;

struct State {
    closed: bool,
    next: u64,
    live: BTreeSet<u64>,
    ready: BTreeSet<u64>,
    served: BTreeSet<u64>,
    cancelled: BTreeSet<u64>,
    tasks: BTreeMap<u64, Work>,
    driver: Option<Arc<Waker>>,
    root_drivers: BTreeMap<u64, Arc<Waker>>,
    random: blake3::OutputReader,
    trace: Vec<(u64, &'static str)>,
    #[cfg(test)]
    forget_wake: bool,
    #[cfg(test)]
    forget_round: bool,
}

impl State {
    fn drivers(&self) -> Drivers {
        let mut drivers = Drivers::default();
        for waker in self.driver.iter().chain(self.root_drivers.values()) {
            if drivers.first.is_none() {
                drivers.first = Some(waker.clone());
            } else if !drivers
                .first
                .iter()
                .chain(&drivers.others)
                .any(|driver| driver.will_wake(waker))
            {
                drivers.others.push(waker.clone());
            }
        }
        drivers
    }
}

// The ordinary single-root path retains one waker without allocating a vector.
#[derive(Default)]
struct Drivers {
    first: Option<Arc<Waker>>,
    others: Vec<Arc<Waker>>,
}

impl IntoIterator for Drivers {
    type Item = Arc<Waker>;
    type IntoIter =
        std::iter::Chain<std::option::IntoIter<Arc<Waker>>, std::vec::IntoIter<Arc<Waker>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.first.into_iter().chain(self.others)
    }
}

/// No future is polled or dropped, and no waker is called, under this lock.
/// This lock is never nested with Schedule, entropy, or task-detail locks.
pub(super) struct Executor(Mutex<State>);

impl Executor {
    pub(super) fn new(seed: u64) -> Arc<Self> {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.dst.poll-order.v1");
        hash.update(&seed.to_le_bytes());
        Arc::new(Self(Mutex::new(State {
            closed: false,
            next: 0,
            live: BTreeSet::new(),
            ready: BTreeSet::new(),
            served: BTreeSet::new(),
            cancelled: BTreeSet::new(),
            tasks: BTreeMap::new(),
            driver: None,
            root_drivers: BTreeMap::new(),
            random: hash.finalize_xof(),
            trace: Vec::new(),
            #[cfg(test)]
            forget_wake: false,
            #[cfg(test)]
            forget_round: false,
        })))
    }

    fn register(self: &Arc<Self>, mut task: Option<Work>) -> Arc<TaskWake> {
        let (id, driver) = {
            let mut state = lock(&self.0);
            let id = state.next;
            state.next = state
                .next
                .checked_add(1)
                .expect("simulation task ID exhausted");
            if !state.closed {
                state.live.insert(id);
                state.ready.insert(id);
                if let Some(task) = task.take() {
                    state.tasks.insert(id, task);
                }
            }
            (id, state.drivers())
        };
        drop(task);
        for driver in driver {
            driver.wake_by_ref();
        }
        Arc::new(TaskWake {
            executor: self.clone(),
            id,
        })
    }

    fn ready(&self, id: u64) {
        let driver = {
            let mut state = lock(&self.0);
            if !state.live.contains(&id) {
                return;
            }
            #[cfg(test)]
            if state.forget_wake {
                return;
            }
            state.ready.insert(id);
            state.drivers()
        };
        for driver in driver {
            driver.wake_by_ref();
        }
    }

    fn retire(&self, id: u64) {
        let (task, retired, drivers) = {
            let mut state = lock(&self.0);
            state.live.remove(&id);
            state.ready.remove(&id);
            state.served.remove(&id);
            state.cancelled.remove(&id);
            let retired = state.root_drivers.remove(&id);
            let task = state.tasks.remove(&id);
            let drivers = if state.ready.is_empty() {
                Drivers::default()
            } else {
                state.drivers()
            };
            (task, retired, drivers)
        };
        drop(retired);
        drop(task);
        for driver in drivers {
            driver.wake_by_ref();
        }
    }

    fn abort(&self, id: u64) {
        let driver = {
            let mut state = lock(&self.0);
            if !state.live.contains(&id) {
                return;
            }
            state.cancelled.insert(id);
            state.ready.insert(id);
            state.drivers()
        };
        for driver in driver {
            driver.wake_by_ref();
        }
    }

    fn next(&self, cx: &PollContext<'_>, root: Option<u64>) -> Option<(u64, Option<Work>, bool)> {
        let drain = root.is_none()
            && !POLLING_EXECUTOR
                .try_with(|executor| std::ptr::eq(executor.as_ref(), self))
                .unwrap_or(false);
        // Raw waker clone and drop callbacks may reenter the executor too.
        // Snapshots retain Arc<Waker>, so neither callback runs under its lock.
        let mut replacement = (root.is_some() || drain).then(|| Arc::new(cx.waker().clone()));
        let (next, retired) = {
            let mut state = lock(&self.0);
            let retired = if state.closed {
                None
            } else if let Some(root) = root {
                state.root_drivers.insert(root, replacement.take().unwrap())
            } else if drain {
                state.driver.replace(replacement.take().unwrap())
            } else {
                None
            };
            let next = if state.closed {
                None
            } else {
                Self::select(&mut state, root)
            };
            (next, retired)
        };
        drop(retired);
        next
    }

    fn select(state: &mut State, root: Option<u64>) -> Option<(u64, Option<Work>, bool)> {
        // Borrowed roots can only be polled by their owning run call. An owned
        // future is eligible only while it is stored here, never while another
        // root is polling it outside the lock.
        let eligible = |state: &State, id: &u64| root == Some(*id) || state.tasks.contains_key(id);
        if !state.ready.iter().any(|id| eligible(state, id)) {
            return None;
        }
        // A self-waking task cannot consume another ready task's entire turn.
        // The seed chooses the order within a round, never whether a task runs.
        #[cfg(test)]
        if state.forget_round {
            state.served.clear();
        }
        if !state
            .ready
            .difference(&state.served)
            .any(|id| eligible(state, id))
        {
            state.served.clear();
        }
        let count = state
            .ready
            .difference(&state.served)
            .filter(|id| eligible(state, id))
            .count();
        let mut draw = [0; 8];
        state.random.fill(&mut draw);
        let index = u64::from_le_bytes(draw) % count as u64;
        let id = *state
            .ready
            .difference(&state.served)
            .filter(|id| eligible(state, id))
            .nth(index as usize)
            .expect("nonempty ready set");
        state.ready.remove(&id);
        state.served.insert(id);
        let cancelled = state.cancelled.contains(&id);
        let task = state.tasks.remove(&id);
        Some((id, task, cancelled))
    }

    fn polled(&self, id: u64, pending: Option<Work>, phase: &'static str) {
        let (discard, driver) = {
            let mut state = lock(&self.0);
            state.trace.push((id, phase));
            let discard = if state.closed || state.cancelled.contains(&id) {
                state.live.remove(&id);
                state.ready.remove(&id);
                state.served.remove(&id);
                state.cancelled.remove(&id);
                pending
            } else {
                if let Some(task) = pending {
                    state.tasks.insert(id, task);
                }
                None
            };
            let driver = if state.ready.is_empty() {
                Drivers::default()
            } else {
                state.drivers()
            };
            (discard, driver)
        };
        drop(discard);
        for driver in driver {
            driver.wake_by_ref();
        }
    }

    fn poll_task(self: &Arc<Self>, id: u64, mut task: Work, cancelled: bool) {
        if cancelled {
            self.retire(id);
            self.polled(id, None, "cancelled");
            drop(task);
            return;
        }
        let waker = Waker::from(Arc::new(TaskWake {
            executor: self.clone(),
            id,
        }));
        let mut cx = PollContext::from_waker(&waker);
        match POLLING_EXECUTOR.sync_scope(self.clone(), || task.as_mut().poll(&mut cx)) {
            Poll::Pending => self.polled(id, Some(task), "pending"),
            Poll::Ready(()) => {
                self.retire(id);
                self.polled(id, None, "ready");
            }
        }
    }

    pub(super) async fn run<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        if POLLING_EXECUTOR
            .try_with(|executor| Arc::ptr_eq(executor, self))
            .unwrap_or(false)
        {
            return future.await;
        }
        let root = Root(self.register(None));
        let waker = Waker::from(root.0.clone());
        let mut future = std::pin::pin!(tokio::task::unconstrained(future));
        std::future::poll_fn(|cx| {
            let Some((id, task, cancelled)) = self.next(cx, Some(root.0.id)) else {
                return Poll::Pending;
            };
            if id == root.0.id {
                assert!(!cancelled, "root cancellation must drop its future");
                let result = POLLING_EXECUTOR.sync_scope(self.clone(), || {
                    future.as_mut().poll(&mut PollContext::from_waker(&waker))
                });
                self.polled(
                    id,
                    None,
                    if result.is_pending() {
                        "pending"
                    } else {
                        "ready"
                    },
                );
                result
            } else {
                self.poll_task(id, task.expect("registered owned task"), cancelled);
                Poll::Pending
            }
        })
        .await
    }

    pub(super) fn poll_owned(self: &Arc<Self>, cx: &PollContext<'_>) {
        if let Some((id, task, cancelled)) = self.next(cx, None) {
            self.poll_task(
                id,
                task.expect("root must be dropped before draining"),
                cancelled,
            );
        }
    }

    pub(super) fn trace(&self) -> Vec<(u64, &'static str)> {
        lock(&self.0).trace.clone()
    }

    pub(super) fn shutdown(&self) {
        let (tasks, driver, roots) = {
            let mut state = lock(&self.0);
            state.closed = true;
            state.live.clear();
            state.ready.clear();
            state.served.clear();
            state.cancelled.clear();
            (
                std::mem::take(&mut state.tasks),
                state.driver.take(),
                std::mem::take(&mut state.root_drivers),
            )
        };
        // Futures can drop contexts or wake receivers; both may reenter us.
        drop((tasks, driver, roots));
    }

    pub(super) fn spawn<F>(self: &Arc<Self>, future: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let task = async move {
            let mut future = std::pin::pin!(tokio::task::unconstrained(future));
            let result = std::future::poll_fn(|cx| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    future.as_mut().poll(cx)
                })) {
                    Ok(poll) => poll.map(Ok),
                    Err(cause) => {
                        let message = cause
                            .downcast_ref::<&str>()
                            .copied()
                            .or_else(|| cause.downcast_ref::<String>().map(String::as_str))
                            .unwrap_or("non-string panic");
                        Poll::Ready(Err(io::Error::other(format!(
                            "owned task panicked: {message}"
                        ))))
                    }
                }
            })
            .await;
            let _ = sender.send(result);
        };
        let wake = self.register(Some(Box::pin(task)));
        TaskHandle(Handle::Simulated { receiver, wake })
    }
}

struct Root(Arc<TaskWake>);
impl Drop for Root {
    fn drop(&mut self) {
        self.0.executor.retire(self.0.id);
    }
}

/// A wake only marks readiness. The seeded driver alone chooses the next poll.
struct TaskWake {
    executor: Arc<Executor>,
    id: u64,
}
impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.executor.ready(self.id);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.executor.ready(self.id);
    }
}

/// Owned task handle. Dropping detaches; aborting cancels the original future.
pub struct TaskHandle<T>(Handle<T>);

/// Cancellation authority retained independently of the owned task's result.
#[derive(Clone)]
pub struct AbortHandle(Abort);

#[derive(Clone)]
enum Abort {
    Tokio(tokio::task::AbortHandle),
    Simulated(Arc<TaskWake>),
}

impl AbortHandle {
    /// Requests cancellation of the original owned future.
    pub fn abort(&self) {
        match &self.0 {
            Abort::Tokio(task) => task.abort(),
            Abort::Simulated(wake) => wake.executor.abort(wake.id),
        }
    }
}

enum Handle<T> {
    Tokio(tokio::task::JoinHandle<T>),
    Simulated {
        receiver: oneshot::Receiver<io::Result<T>>,
        wake: Arc<TaskWake>,
    },
}

impl<T> TaskHandle<T> {
    pub(super) fn native(task: tokio::task::JoinHandle<T>) -> Self {
        Self(Handle::Tokio(task))
    }

    /// Cancels the task even if its caller no longer awaits it.
    pub fn abort(&self) {
        match &self.0 {
            Handle::Tokio(task) => task.abort(),
            Handle::Simulated { wake, .. } => wake.executor.abort(wake.id),
        }
    }

    /// Retains cancellation authority without retaining the result receiver.
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle(match &self.0 {
            Handle::Tokio(task) => Abort::Tokio(task.abort_handle()),
            Handle::Simulated { wake, .. } => Abort::Simulated(wake.clone()),
        })
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = io::Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut PollContext<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().0 {
            Handle::Tokio(task) => Pin::new(task)
                .poll(cx)
                .map(|result| result.map_err(crate::task::join_error)),
            Handle::Simulated { receiver, .. } => Pin::new(receiver).poll(cx).map(|result| {
                result.unwrap_or_else(|_| {
                    Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "owned task cancelled",
                    ))
                })
            }),
        }
    }
}

// This is the external I/O completion seam, not another implementation of I/O.
// Poll the real queue-admission/worker/terminal future to physical completion
// between two simulated steps, so kernel latency cannot change ready-task order.
pub(super) fn complete_io<F: Future>(future: F) -> F::Output {
    let wake = Arc::new(CompletionWake {
        ready: Mutex::new(false),
        signal: Condvar::new(),
        #[cfg(test)]
        omit_latch: false,
    });
    wait_io(future, wake)
}

fn wait_io<F: Future>(future: F, wake: Arc<CompletionWake>) -> F::Output {
    let waker = Waker::from(wake.clone());
    let mut cx = PollContext::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        let mut ready = wake.ready.lock().unwrap_or_else(PoisonError::into_inner);
        while !*ready {
            ready = wake
                .signal
                .wait(ready)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *ready = false;
    }
}

struct CompletionWake {
    // Sole lock; no nesting with executor/queue locks. Condvar wait releases it.
    ready: Mutex<bool>,
    signal: Condvar,
    #[cfg(test)]
    omit_latch: bool,
}

impl Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        #[cfg(test)]
        if self.omit_latch {
            self.signal.notify_one();
            return;
        }
        *lock(&self.ready) = true;
        self.signal.notify_one();
    }
}
