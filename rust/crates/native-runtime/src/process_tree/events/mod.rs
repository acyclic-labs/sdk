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

mod outcome;
mod signal;
pub use outcome::ProcessWaitOutcome;
use signal::Signal;

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
mod health_tests;
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

struct Armed {
    signal: Arc<Signal>,
    wake_at: Option<Instant>,
}

#[derive(Default)]
struct Registry {
    next: u64,
    armed: HashMap<u64, Armed>,
    timers: BTreeSet<(Instant, u64)>,
    failure: Option<Arc<io::Error>>,
}

struct Hub {
    poller: platform::Poller,
    registry: Mutex<Registry>,
}

/// Unwinding or returning from the sole producer has the same terminal path.
struct Producer<'a>(&'a Hub);

impl Drop for Producer<'_> {
    fn drop(&mut self) {
        self.0
            .fail(io::Error::other("process readiness service stopped"));
    }
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
    HUB.as_ref()
        .copied()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

impl Hub {
    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn run(&self) {
        let _producer = Producer(self);
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
            if !self.accept(self.poller.wait(timeout, &mut ready)) {
                return;
            }
            self.dispatch(&mut ready, Instant::now());
        }
    }

    fn accept(&self, result: io::Result<()>) -> bool {
        match result {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => true,
            Err(error) => {
                self.fail(error);
                false
            }
        }
    }

    fn fail(&self, error: io::Error) {
        let (waits, error) = {
            let mut registry = self.registry();
            if registry.failure.is_some() {
                return;
            }
            let error = Arc::new(error);
            registry.failure = Some(Arc::clone(&error));
            registry.timers.clear();
            (std::mem::take(&mut registry.armed), error)
        };
        // One terminal fanout; normal delivery still visits only ready tokens.
        // Notification never calls a consumer while holding the registry lock.
        for armed in waits.into_values() {
            armed
                .signal
                .settle(ProcessWaitOutcome::ServiceFailed(Arc::clone(&error)));
        }
    }

    fn dispatch(&self, ready: &mut Vec<u64>, now: Instant) {
        let mut fired = Vec::new();
        {
            let mut registry = self.registry();
            for token in ready.drain(..) {
                if let Some(armed) = registry.armed.remove(&token) {
                    if let Some(at) = armed.wake_at {
                        registry.timers.remove(&(at, token));
                    }
                    fired.push((armed.signal, ProcessWaitOutcome::Ready));
                }
            }
            while let Some(&(at, token)) = registry.timers.first() {
                if at > now {
                    break;
                }
                registry.timers.remove(&(at, token));
                if let Some(armed) = registry.armed.remove(&token) {
                    fired.push((armed.signal, ProcessWaitOutcome::Deadline));
                }
            }
        }
        for (signal, outcome) in fired {
            signal.settle(outcome);
        }
    }
}

/// One armed wait. It completes once, when any of its sources is ready or its
/// wake time passes; dropping it disarms every registration it still holds.
pub(super) struct Wait {
    hub: &'static Hub,
    token: u64,
    signal: Arc<Signal>,
    _registrations: Vec<platform::Registration>,
}

/// Arms one wait on `sources`, waking no later than `wake_at`.
pub(super) fn arm(sources: &[Source<'_>], wake_at: Option<Instant>) -> io::Result<Wait> {
    let hub = hub()?;
    hub.arm(sources, wake_at)
}

impl Hub {
    fn arm(&'static self, sources: &[Source<'_>], wake_at: Option<Instant>) -> io::Result<Wait> {
        let hub = self;
        let signal = Arc::new(Signal::default());
        let (token, earliest) = {
            let mut registry = hub.registry();
            if let Some(error) = &registry.failure {
                signal.settle(ProcessWaitOutcome::ServiceFailed(Arc::clone(error)));
                return Ok(Wait {
                    hub,
                    token: 0,
                    signal,
                    _registrations: Vec::new(),
                });
            }
            if sources.is_empty() && wake_at.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "process readiness needs a source or deadline",
                ));
            }
            registry.next = registry
                .next
                .checked_add(1)
                .filter(|token| *token != u64::MAX)
                .ok_or_else(|| io::Error::other("process readiness tokens exhausted"))?;
            let token = registry.next;
            let earliest =
                wake_at.filter(|at| registry.timers.first().is_none_or(|(first, _)| at < first));
            if let Some(at) = wake_at {
                registry.timers.insert((at, token));
            }
            registry.armed.insert(
                token,
                Armed {
                    signal: Arc::clone(&signal),
                    wake_at,
                },
            );
            (token, earliest)
        };
        let mut wait = Wait {
            hub,
            token,
            signal,
            _registrations: Vec::with_capacity(sources.len()),
        };
        for source in sources {
            let Some(registration) = hub.poller.register(*source, token)? else {
                // The source is ready already.
                wait.signal.settle(ProcessWaitOutcome::Ready);
                return Ok(wait);
            };
            wait._registrations.push(registration);
        }
        if earliest.is_some() {
            loop {
                match hub.poller.notify() {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => {
                        hub.fail(error);
                        break;
                    }
                    Ok(()) => break,
                }
            }
        }
        Ok(wait)
    }
}

impl Wait {
    /// Whether a source or the wake time has fired.
    #[cfg(test)]
    pub(super) fn fired(&self) -> bool {
        self.signal
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    pub(super) fn cancel(&self) {
        self.signal.settle(ProcessWaitOutcome::Cancelled);
    }

    /// Parks the calling thread until the wait fires.
    pub(super) fn block(self) -> ProcessWaitOutcome {
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(outcome) = self.signal.poll(&mut context) {
                return outcome;
            }
            std::thread::park();
        }
    }
}

impl Future for Wait {
    type Output = ProcessWaitOutcome;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.signal.poll(context)
    }
}

impl Drop for Wait {
    fn drop(&mut self) {
        let mut registry = self.hub.registry();
        if let Some(armed) = registry.armed.remove(&self.token)
            && let Some(at) = armed.wake_at
        {
            registry.timers.remove(&(at, self.token));
        }
    }
}

/// The number of armed waits the hub holds, for scale tests.
#[cfg(all(test, feature = "test-support", not(loom)))]
pub(super) fn armed_waits() -> usize {
    hub().map_or(0, |hub| hub.registry().armed.len())
}
