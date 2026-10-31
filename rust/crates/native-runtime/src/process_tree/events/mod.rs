//! One process-wide readiness hub for every process wait.
//!
//! A waiting child holds no thread. Each wait arms one-shot interest in its
//! pipes, its exit and an optional wake time with a single hub thread (epoll
//! with pidfds on Linux, kqueue with `EVFILT_PROC` on macOS, an I/O completion
//! port with pipe operations and registered process waits on Windows), and
//! the first of them to fire wakes the waiter. An async waiter costs a waker;
//! a synchronous caller parks its own thread.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::io;
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

#[cfg(target_os = "linux")]
#[path = "linux.rs"]
mod platform;
#[cfg(target_vendor = "apple")]
#[path = "apple.rs"]
mod platform;
#[cfg(windows)]
#[path = "windows.rs"]
mod platform;

#[cfg(windows)]
pub(super) use platform::PipeOperation;

#[cfg(test)]
mod tests;

/// One readiness source of a single wait.
#[derive(Clone, Copy)]
pub(super) enum Source<'a> {
    /// Data, end of file or an error is pending on this pipe.
    #[cfg(unix)]
    Readable(std::os::fd::BorrowedFd<'a>),
    /// This pipe accepts more bytes, or its reader has gone.
    #[cfg(unix)]
    Writable(std::os::fd::BorrowedFd<'a>),
    /// The process behind this pidfd has exited.
    #[cfg(target_os = "linux")]
    Exit(std::os::fd::BorrowedFd<'a>),
    /// The process with this ID has exited.
    #[cfg(target_vendor = "apple")]
    Exit(libc::pid_t, std::marker::PhantomData<&'a ()>),
    /// The process or other waitable object behind this handle is signaled.
    #[cfg(windows)]
    Exit(std::os::windows::io::BorrowedHandle<'a>),
    /// This pipe operation has completed, or the pipe has data to read.
    #[cfg(windows)]
    Pipe(&'a Arc<PipeOperation>),
}

mod outcome;
mod signal;
pub use outcome::ProcessWaitOutcome;
use signal::Signal;

fn copy_error(error: &io::Error) -> io::Error {
    error.raw_os_error().map_or_else(
        || io::Error::new(error.kind(), error.to_string()),
        io::Error::from_raw_os_error,
    )
}

struct Armed {
    signal: Arc<Signal>,
    wake_at: Option<Instant>,
}

#[derive(Default)]
struct Registry {
    next: u64,
    // A dense live list makes terminal notification proportional only to the
    // waiters it must settle, independent of cancelled/historical entries.
    armed: Vec<(u64, Armed)>,
    positions: HashMap<u64, usize>,
    timers: BTreeSet<(Instant, u64)>,
    failure: Option<Arc<io::Error>>,
}

impl Registry {
    fn registered(
        &mut self,
        poller: &platform::Poller,
        token: u64,
        signal: &Arc<Signal>,
        wake_at: Option<Instant>,
        ready: bool,
    ) -> Option<Failure> {
        if ready {
            return None;
        }
        let earliest = wake_at.filter(|at| self.timers.first().is_none_or(|(first, _)| at < first));
        if let Some(at) = wake_at {
            self.timers.insert((at, token));
        }
        self.positions.insert(token, self.armed.len());
        self.armed.push((
            token,
            Armed {
                signal: Arc::clone(signal),
                wake_at,
            },
        ));
        earliest
            .and_then(|_| poller.notify().err())
            .and_then(|error| self.fail(error))
    }

    fn remove(&mut self, token: u64) -> Option<Armed> {
        let position = self.positions.remove(&token)?;
        let (_, armed) = self.armed.swap_remove(position);
        if let Some((moved, _)) = self.armed.get(position) {
            self.positions.insert(*moved, position);
        }
        if let Some(at) = armed.wake_at {
            self.timers.remove(&(at, token));
        }
        Some(armed)
    }

    fn fail(&mut self, error: io::Error) -> Option<Failure> {
        if self.failure.is_some() {
            return None;
        }
        let error = Arc::new(error);
        self.failure = Some(Arc::clone(&error));
        self.positions = HashMap::new();
        self.timers.clear();
        Some(Failure {
            error,
            armed: std::mem::take(&mut self.armed),
        })
    }
}

struct Failure {
    error: Arc<io::Error>,
    armed: Vec<(u64, Armed)>,
}

impl Failure {
    fn deliver(self) {
        deliver(self.armed.into_iter().map(|(_, armed)| {
            (
                armed.signal,
                ProcessWaitOutcome::ServiceFailed(Arc::clone(&self.error)),
            )
        }));
    }
}

struct Hub {
    poller: platform::Poller,
    registry: Mutex<Registry>,
}

#[cfg(test)]
static STARTED: AtomicBool = AtomicBool::new(false);

/// Whether anything in this process has started the hub.
#[cfg(test)]
fn started() -> bool {
    STARTED.load(Ordering::Acquire)
}

static HUB: LazyLock<io::Result<&'static Hub>> = LazyLock::new(|| {
    #[cfg(test)]
    STARTED.store(true, Ordering::Release);
    let hub: &'static Hub = Box::leak(Box::new(Hub {
        poller: platform::Poller::new()?,
        registry: Mutex::new(Registry::default()),
    }));
    std::thread::Builder::new()
        .name("acyclic-process-events".into())
        .spawn(move || hub.run())?;
    Ok(hub)
});

/// The running hub, held by every process tree from before its child is
/// created. The hub is therefore part of the first spawn, never of whichever
/// later wait happens to arm first: a process's service threads and
/// descriptors do not depend on whether its children exited before anyone
/// waited, and a hub that cannot start fails the spawn, not a later wait.
#[derive(Clone, Copy)]
pub(super) struct Events {
    _hub: &'static Hub,
}

impl Events {
    pub(super) fn start() -> io::Result<Self> {
        let hub = hub()?;
        if let Some(error) = &hub.registry().failure {
            return Err(io::Error::new(error.kind(), Arc::clone(error)));
        }
        Ok(Self { _hub: hub })
    }
}

fn hub() -> io::Result<&'static Hub> {
    HUB.as_ref().copied().map_err(copy_error)
}

impl Hub {
    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn fail(&self, error: io::Error) {
        let failure = self.registry().fail(error);
        if let Some(failure) = failure {
            failure.deliver();
        }
    }

    fn run(&self) {
        let _retirement = Retirement(self);
        let mut ready = Vec::new();
        loop {
            let timeout = {
                let registry = self.registry();
                if registry.failure.is_some() {
                    return;
                }
                registry
                    .timers
                    .first()
                    .map(|(at, _)| at.saturating_duration_since(Instant::now()))
            };
            ready.clear();
            if let Err(error) = self.poller.wait(timeout, &mut ready) {
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                self.fail(error);
                return;
            }
            self.dispatch(&mut ready, Instant::now());
        }
    }

    fn dispatch(&self, ready: &mut Vec<u64>, now: Instant) {
        let mut fired = Vec::new();
        {
            let mut registry = self.registry();
            for token in ready.drain(..) {
                if let Some(armed) = registry.remove(token) {
                    fired.push((armed.signal, ProcessWaitOutcome::Ready));
                }
            }
            while let Some(&(at, token)) = registry.timers.first() {
                if at > now {
                    break;
                }
                if let Some(armed) = registry.remove(token) {
                    fired.push((armed.signal, ProcessWaitOutcome::Deadline));
                } else {
                    registry.timers.remove(&(at, token));
                }
            }
        }
        deliver(fired.into_iter());
    }
}

// Removed signals retain their terminal outcomes until delivered.
struct Delivery<I: Iterator<Item = (Arc<Signal>, ProcessWaitOutcome)>> {
    signals: I,
}

impl<I: Iterator<Item = (Arc<Signal>, ProcessWaitOutcome)>> Drop for Delivery<I> {
    fn drop(&mut self) {
        for (signal, outcome) in self.signals.by_ref() {
            signal.settle(outcome);
        }
    }
}

fn deliver(signals: impl Iterator<Item = (Arc<Signal>, ProcessWaitOutcome)>) {
    let mut delivery = Delivery { signals };
    for (signal, outcome) in delivery.signals.by_ref() {
        signal.settle(outcome);
    }
}

// A producer that unwinds also settles its waits; no replacement is started.
struct Retirement<'a>(&'a Hub);

impl Drop for Retirement<'_> {
    fn drop(&mut self) {
        self.0.fail(io::ErrorKind::BrokenPipe.into());
    }
}

/// One armed wait. It completes once, when any of its sources is ready or its
/// wake time passes, or fails when its OS event producer ends. Dropping it
/// disarms every registration it still holds.
pub(super) struct Wait<'a> {
    hub: &'a Hub,
    token: u64,
    signal: Arc<Signal>,
    _registrations: Vec<platform::Registration>,
}

/// Arms one wait on `sources`, waking no later than `wake_at`.
pub(super) fn arm(sources: &[Source<'_>], wake_at: Option<Instant>) -> io::Result<Wait<'static>> {
    if sources.is_empty() && wake_at.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process readiness needs a source or deadline",
        ));
    }
    arm_on(hub()?, sources, wake_at)
}

fn arm_on<'a>(
    hub: &'a Hub,
    sources: &[Source<'_>],
    wake_at: Option<Instant>,
) -> io::Result<Wait<'a>> {
    let signal = Arc::new(Signal::default());
    let mut registrations = Vec::new();
    let (token, ready, failure) = {
        let mut registry = hub.registry();
        if let Some(error) = &registry.failure {
            let error = Arc::clone(error);
            drop(registry);
            signal.settle(ProcessWaitOutcome::ServiceFailed(error));
            return Ok(Wait {
                hub,
                token: 0,
                signal,
                _registrations: registrations,
            });
        }
        registry.next = registry
            .next
            .checked_add(1)
            .filter(|token| *token != u64::MAX)
            .ok_or_else(|| io::Error::other("process readiness tokens exhausted"))?;
        registrations.reserve(sources.len());
        let token = registry.next;
        // Registration and producer retirement share this lifecycle lock.
        // No native registration may be dispatched after its producer ends.
        let registration = (|| {
            for source in sources {
                let Some(registration) = hub.poller.register(*source, token)? else {
                    return Ok(true);
                };
                registrations.push(registration);
            }
            Ok(false)
        })();
        match registration {
            Ok(ready) => {
                let failure = registry.registered(&hub.poller, token, &signal, wake_at, ready);
                (token, ready, failure)
            }
            Err(error) => return Err(error),
        }
    };
    let wait = Wait {
        hub,
        token,
        signal,
        _registrations: registrations,
    };
    // A wake may re-enter registration, so deliver outside the lifecycle lock.
    if let Some(failure) = failure {
        failure.deliver();
    } else if ready {
        wait.signal.settle(ProcessWaitOutcome::Ready);
    }
    Ok(wait)
}

impl Wait<'_> {
    pub(super) fn cancel(&self) {
        self.signal.settle(ProcessWaitOutcome::Cancelled);
    }
    /// Whether a source, the wake time or producer failure has settled.
    #[cfg(test)]
    pub(super) fn fired(&self) -> bool {
        self.signal
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    /// Parks the calling thread until readiness or its producer's failure.
    pub(super) fn block(mut self) -> ProcessWaitOutcome {
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(result) = Pin::new(&mut self).poll(&mut context) {
                return result;
            }
            std::thread::park();
        }
    }
}

impl Future for Wait<'_> {
    type Output = ProcessWaitOutcome;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.signal.poll(context)
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        self.hub.registry().remove(self.token);
    }
}

/// The number of armed waits the hub holds, for scale tests.
#[cfg(all(test, feature = "test-support", not(loom)))]
pub(super) fn armed_waits() -> usize {
    hub().map_or(0, |hub| hub.registry().armed.len())
}
