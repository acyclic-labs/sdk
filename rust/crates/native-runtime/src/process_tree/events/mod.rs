//! One process-wide readiness hub for every process wait.
//!
//! A waiting child holds no thread. Each wait arms one-shot interest in its
//! pipes, its exit and an optional wake time with a single hub thread (epoll
//! with pidfds on Linux, kqueue with `EVFILT_PROC` on macOS, an I/O completion
//! port with pipe operations and registered process waits on Windows), and
//! the first of them to fire wakes the waiter. An async waiter costs a waker;
//! a synchronous caller parks its own thread.

use std::collections::{BTreeSet, HashMap, HashSet};
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
    sources: HashSet<SourceKey>,
    #[cfg(test)]
    lock_count: usize,
}

struct Hub {
    poller: platform::Poller,
    registry: Mutex<Registry>,
}

/// Producer loss uses the same terminal path as a fatal kernel error.
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
        let registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
        #[cfg(test)]
        let registry = {
            let mut registry = registry;
            registry.lock_count += 1;
            registry
        };
        registry
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
        let error = Arc::new(error);
        let armed = {
            let mut registry = self.registry();
            if registry.failure.is_some() {
                return;
            }
            registry.failure = Some(Arc::clone(&error));
            registry.timers.clear();
            std::mem::take(&mut registry.armed)
        };
        // Fanout is necessarily O(W), once per failed service. No callbacks
        // run under the registry lock; future arms observe the retained error.
        for armed in armed.into_values() {
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
pub(super) struct Wait<'a> {
    hub: &'static Hub,
    token: u64,
    signal: Arc<Signal>,
    registrations: Vec<(SourceKey, Option<platform::Registration>)>,
    // A registration cannot outlive or be replaced through its source owner.
    sources: std::marker::PhantomData<&'a ()>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct SourceKey(usize, u8);

impl Source<'_> {
    fn key(self) -> SourceKey {
        #[cfg(unix)]
        use std::os::fd::AsRawFd as _;
        #[cfg(windows)]
        use std::os::windows::io::AsRawHandle as _;
        match self {
            #[cfg(unix)]
            Self::Readable(fd) => SourceKey(fd.as_raw_fd().cast_unsigned() as usize, 0),
            #[cfg(unix)]
            Self::Writable(fd) => SourceKey(
                fd.as_raw_fd().cast_unsigned() as usize,
                if cfg!(target_os = "linux") { 0 } else { 1 },
            ),
            #[cfg(target_os = "linux")]
            Self::Exit(fd) => SourceKey(fd.as_raw_fd().cast_unsigned() as usize, 0),
            #[cfg(target_vendor = "apple")]
            Self::Exit(pid, _) => SourceKey(pid.cast_unsigned() as usize, 2),
            #[cfg(windows)]
            Self::Exit(handle) => SourceKey(handle.as_raw_handle().addr(), 2),
            #[cfg(windows)]
            Self::Pipe(operation) => SourceKey(Arc::as_ptr(operation).addr(), 0),
        }
    }
}

/// Arms one wait on `sources`, waking no later than `wake_at`.
pub(super) fn arm<'a>(sources: &[Source<'a>], wake_at: Option<Instant>) -> io::Result<Wait<'a>> {
    let hub = hub()?;
    hub.arm(sources, wake_at)
}

impl Hub {
    fn arm<'a>(
        &'static self,
        sources: &[Source<'a>],
        wake_at: Option<Instant>,
    ) -> io::Result<Wait<'a>> {
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
                    registrations: Vec::new(),
                    sources: std::marker::PhantomData,
                });
            }
            if sources.is_empty() && wake_at.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "process readiness needs a source or deadline; retain a producer before arming",
                ));
            }
            registry.next = registry
                .next
                .checked_add(1)
                .filter(|token| *token != u64::MAX)
                .ok_or_else(|| io::Error::other("process readiness tokens exhausted"))?;
            let token = registry.next;
            for (index, source) in sources.iter().enumerate() {
                if !registry.sources.insert(source.key()) {
                    for previous in sources.iter().take(index) {
                        registry.sources.remove(&previous.key());
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "process readiness source is already armed; drop its previous wait before rearming",
                    ));
                }
            }
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
            registrations: sources.iter().map(|source| (source.key(), None)).collect(),
            sources: std::marker::PhantomData,
        };
        for (source, (_, slot)) in sources.iter().zip(&mut wait.registrations) {
            let registration = loop {
                match hub.poller.register(*source, token) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    result => break result?,
                }
            };
            let Some(registration) = registration else {
                // The source is ready already.
                wait.signal.settle(ProcessWaitOutcome::Ready);
                return Ok(wait);
            };
            *slot = Some(registration);
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

impl Wait<'_> {
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

impl Future for Wait<'_> {
    type Output = ProcessWaitOutcome;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.signal.poll(context)
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        let mut failure = None;
        let mut registry = self.hub.registry();
        if let Some(armed) = registry.armed.remove(&self.token)
            && let Some(at) = armed.wake_at
        {
            registry.timers.remove(&(at, self.token));
        }
        // Kernel deletion must precede allowing another registration to reuse
        // this source. Callbacks only post tokens and never acquire this lock.
        for (key, mut registration) in self.registrations.drain(..) {
            if let Some(registration) = registration.as_mut()
                && let Err(error) = registration.disarm()
            {
                failure.get_or_insert(error);
            }
            drop(registration);
            registry.sources.remove(&key);
        }
        drop(registry);
        if let Some(error) = failure {
            self.hub.fail(error);
        }
    }
}

/// The number of armed waits the hub holds, for scale tests.
#[cfg(all(test, feature = "test-support", not(loom)))]
pub(super) fn armed_waits() -> usize {
    hub().map_or(0, |hub| hub.registry().armed.len())
}
