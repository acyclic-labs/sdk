//! A real host worker yields only at an original blocking future's Pending.
use super::*;
#[cfg(acyclic_native_runtime_loom)]
use loom::sync::Condvar;
#[cfg(not(acyclic_native_runtime_loom))]
use std::sync::Condvar;
use std::task::{Context as PollContext, Wake};
mod pool;
pub(crate) use pool::Pool;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Starting,
    Running,
    Parked,
    Finished,
}

struct State {
    phase: Phase,
    ready: bool,
    io_ready: bool,
    waiting_start: bool,
    detached: bool,
    driver: Option<Waker>,
    #[cfg(test)]
    forget_ready: bool,
    #[cfg(test)]
    forget_io: bool,
}

// Sole handoff lock. Never nested with executor, schedule, channel or provider
// locks; future polls, callbacks, wakes and original I/O happen outside it.
pub(super) struct Host {
    state: Mutex<State>,
    changed: Condvar,
    pool: Arc<pool::Pool>,
}

// Installed by the receiving worker around the original Task callback.
pub(crate) struct Work {
    host: Option<Arc<Host>>,
    context: Option<Context>,
}

impl Work {
    pub(super) fn bind(
        job: &mut crate::NativeJob,
        context: Option<Context>,
        host: Option<Arc<Host>>,
    ) {
        if let crate::NativeJob::Task { scope, .. } = job {
            *scope = (context.is_some() || host.is_some()).then(|| Self { host, context });
        }
    }

    pub(crate) fn controlled(&self) -> bool {
        self.host.is_some()
    }

    pub(crate) fn run<T>(self, work: impl FnOnce() -> T) -> T {
        let Self { host, mut context } = self;
        if let Some(context) = &mut context {
            context.host = None;
        }
        BLOCKING_CONTEXT.sync_scope(context, || match host {
            Some(host) => host.run(work),
            None => work(),
        })
    }
}

impl Host {
    pub(super) fn current() -> Option<Arc<Self>> {
        BLOCKING_CONTEXT
            .try_with(|context| context.as_ref().and_then(|context| context.host.clone()))
            .ok()
            .flatten()
    }
    #[cfg(test)]
    #[cfg(acyclic_native_runtime_loom)]
    pub(super) fn new(threads: usize) -> Arc<Self> {
        Self::with_pool(Pool::new(threads))
    }

    pub(super) fn for_workers(workers: &crate::NativeWorkers) -> Arc<Self> {
        Self::with_pool(workers.host_pool.clone())
    }

    fn with_pool(pool: Arc<Pool>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                phase: Phase::Starting,
                ready: false,
                io_ready: false,
                waiting_start: false,
                detached: false,
                driver: None,
                #[cfg(test)]
                forget_ready: false,
                #[cfg(test)]
                forget_io: false,
            }),
            changed: Condvar::new(),
            pool,
        })
    }

    pub(super) fn run<T>(self: &Arc<Self>, work: impl FnOnce() -> T) -> T {
        lock(&self.state).phase = Phase::Running;
        self.pool.changed();
        let _finished = Finished(self.clone());
        let context = Context::simulation().map(|mut context| {
            context.host = Some(self.clone());
            context
        });
        BLOCKING_CONTEXT.sync_scope(context, work)
    }

    fn block<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        let waker = Waker::from(self.clone());
        let mut cx = PollContext::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            // Synchronous physical work retains sources only. Its explicit
            // async callback must also retain task ownership: submissions can
            // yield this worker's permit at Pending and run in seeded order.
            let context = Context::simulation();
            if let Poll::Ready(output) =
                CURRENT.sync_scope(context, || future.as_mut().poll(&mut cx))
            {
                return output;
            }
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.phase = Phase::Parked;
            let saturated = self.pool.park();
            // Parking is not readiness: only the original future's wake or
            // physical terminal may enqueue the next seeded task poll.
            state.waiting_start = false;
            if state.detached && state.ready {
                state.ready = false;
                self.pool.resume();
                state.phase = Phase::Running;
            }
            self.changed.notify_one();
            drop(state);
            if saturated {
                self.pool.changed();
            }
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            while state.phase == Phase::Parked {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }

    // Wait for the physical worker to finish or reach an original async wait.
    // Root/owned futures can then satisfy that wait without any recursive poll.
    pub(super) async fn drive<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        let _driver = Driver(self.clone());
        let mut future = std::pin::pin!(future);
        let io_waker = Waker::from(Arc::new(IoWake(self.clone())));
        std::future::poll_fn(|cx| {
            'poll_io: loop {
                if let Poll::Ready(output) = future
                    .as_mut()
                    .poll(&mut PollContext::from_waker(&io_waker))
                {
                    return Poll::Ready(output);
                }
                let mut state = self.pool.start_or_saturation(&self.state);
                state.driver = Some(cx.waker().clone());
                loop {
                    if state.io_ready {
                        state.io_ready = false;
                        drop(state);
                        continue 'poll_io;
                    }
                    match state.phase {
                        Phase::Finished => {
                            drop(state);
                            return Poll::Ready(executor::complete_io(future.as_mut()));
                        }
                        Phase::Parked if state.ready => {
                            state.ready = false;
                            self.pool.resume();
                            state.phase = Phase::Running;
                            self.changed.notify_one();
                        }
                        Phase::Parked => return Poll::Pending,
                        Phase::Starting => {
                            state.waiting_start = true;
                            return Poll::Pending;
                        }
                        _ => (),
                    }
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        })
        .await
    }
}

// Accepted physical work outlives its replay driver. Once that driver goes
// away, original wakes resume the worker directly instead of queuing a poll
// on an executor that can no longer run it.
struct Driver(Arc<Host>);
impl Drop for Driver {
    fn drop(&mut self) {
        // Loom destroys suspended coroutine stacks after reporting a modeled
        // deadlock. Its synchronization primitives are no longer usable in
        // that cleanup. Ordinary panic unwinding must still detach the host.
        #[cfg(acyclic_native_runtime_loom)]
        if std::thread::panicking() {
            return;
        }
        let driver = {
            let mut state = lock(&self.0.state);
            state.detached = true;
            if state.phase == Phase::Parked && state.ready {
                state.ready = false;
                self.0.pool.resume();
                state.phase = Phase::Running;
                self.0.changed.notify_one();
            }
            state.driver.take()
        };
        drop(driver);
    }
}

// Queue capacity and physical completion wakes stay inside the I/O step;
// device latency must not enqueue an extra simulated task poll.
struct IoWake(Arc<Host>);
impl Wake for IoWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let driver = {
            let mut state = lock(&self.0.state);
            #[cfg(test)]
            if state.forget_io {
                return;
            }
            state.io_ready = true;
            (
                state.phase == Phase::Starting,
                state.waiting_start.then(|| state.driver.clone()).flatten(),
            )
        };
        self.0.changed.notify_one();
        if driver.0 {
            self.0.pool.changed();
        }
        let driver = driver.1;
        if let Some(driver) = driver {
            driver.wake();
        }
    }
}

struct Finished(Arc<Host>);
impl Drop for Finished {
    fn drop(&mut self) {
        let driver = {
            let mut state = lock(&self.0.state);
            state.phase = Phase::Finished;
            let driver = state.waiting_start.then(|| state.driver.clone()).flatten();
            state.waiting_start = false;
            driver
        };
        self.0.changed.notify_one();
        if let Some(driver) = driver {
            driver.wake();
        }
    }
}

impl Wake for Host {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let driver = {
            let mut state = lock(&self.state);
            #[cfg(test)]
            if state.forget_ready {
                return;
            }
            state.ready = true;
            if state.detached && state.phase == Phase::Parked {
                state.ready = false;
                self.pool.resume();
                state.phase = Phase::Running;
                self.changed.notify_one();
            }
            state.driver.clone()
        };
        if let Some(driver) = driver {
            driver.wake();
        }
    }
}

/// Waits on the original channel/control future on its real blocking worker.
/// A simulated worker yields its permit at Pending and resumes by seed order.
pub fn blocking_on<F: Future>(future: F) -> F::Output {
    match Context::simulation().and_then(|context| context.host) {
        Some(host) => host.block(future),
        None => executor::complete_io(future),
    }
}

#[cfg(test)]
#[cfg(acyclic_native_runtime_loom)]
mod models;
#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod tests;
