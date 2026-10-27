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

/// The one-shot state shared by an armed wait and the hub that fires it.
struct Signal {
    fired: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Signal {
    fn fire(&self) {
        self.fired.store(true, Ordering::Release);
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
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
        hub().map(|hub| Self { _hub: hub })
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
        let mut ready = Vec::new();
        loop {
            let timeout = self
                .registry()
                .timers
                .first()
                .map(|(at, _)| at.saturating_duration_since(Instant::now()));
            ready.clear();
            // A failed poll fires nothing; every waiter still has its own
            // wake time, and the next turn retries the same kernel wait.
            let _ = self.poller.wait(timeout, &mut ready);
            let now = Instant::now();
            let mut fired = Vec::new();
            {
                let mut registry = self.registry();
                for token in ready.drain(..) {
                    if let Some(armed) = registry.armed.remove(&token) {
                        if let Some(at) = armed.wake_at {
                            registry.timers.remove(&(at, token));
                        }
                        fired.push(armed.signal);
                    }
                }
                while let Some(&(at, token)) = registry.timers.first() {
                    if at > now {
                        break;
                    }
                    registry.timers.remove(&(at, token));
                    if let Some(armed) = registry.armed.remove(&token) {
                        fired.push(armed.signal);
                    }
                }
            }
            for signal in fired {
                signal.fire();
            }
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
    let signal = Arc::new(Signal {
        fired: AtomicBool::new(false),
        waker: Mutex::new(None),
    });
    let (token, earliest) = {
        let mut registry = hub.registry();
        registry.next = registry.next.wrapping_add(1);
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
            wait.signal.fired.store(true, Ordering::Release);
            return Ok(wait);
        };
        wait._registrations.push(registration);
    }
    if earliest.is_some() {
        hub.poller.notify()?;
    }
    Ok(wait)
}

impl Wait {
    /// Whether a source or the wake time has fired.
    pub(super) fn fired(&self) -> bool {
        self.signal.fired.load(Ordering::Acquire)
    }

    /// Parks the calling thread until the wait fires.
    pub(super) fn block(self) {
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        *self
            .signal
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(waker);
        while !self.fired() {
            std::thread::park();
        }
    }
}

impl Future for Wait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.fired() {
            return Poll::Ready(());
        }
        *self
            .signal
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(context.waker().clone());
        if self.fired() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
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
