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
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
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

// One native interest has one owner. In particular, epoll stores one token
// per descriptor, even when callers ask for different readiness directions.
#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum SourceKey {
    Descriptor(std::os::fd::RawFd),
    #[cfg(target_vendor = "apple")]
    Process(libc::pid_t),
}

#[cfg(unix)]
impl Source<'_> {
    fn key(self) -> SourceKey {
        use std::os::fd::AsRawFd as _;
        match self {
            Self::Readable(fd) | Self::Writable(fd) => SourceKey::Descriptor(fd.as_raw_fd()),
            #[cfg(target_os = "linux")]
            Self::Exit(fd) => SourceKey::Descriptor(fd.as_raw_fd()),
            #[cfg(target_vendor = "apple")]
            Self::Exit(pid, _) => SourceKey::Process(pid),
        }
    }
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
    #[cfg(unix)]
    sources: HashMap<SourceKey, u64>,
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

static HUB: OnceLock<io::Result<&'static Hub>> = OnceLock::new();

fn start_hub() -> io::Result<&'static Hub> {
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
}

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
    HUB.get_or_init(start_hub)
        .as_ref()
        .copied()
        .map_err(copy_error)
}

// Source retirement precedes physical close or ownership transfer. An old
// wait may outlive that source, but must not reserve a reused descriptor/PID.
#[cfg(unix)]
fn retire_source(source: SourceKey) {
    let Some(Ok(hub)) = HUB.get() else {
        return;
    };
    let signal = {
        let mut registry = hub.registry();
        registry
            .sources
            .remove(&source)
            .and_then(|token| registry.remove(token))
            .map(|armed| armed.signal)
    };
    if let Some(signal) = signal {
        signal.settle(ProcessWaitOutcome::Cancelled);
    }
}

#[cfg(target_vendor = "apple")]
pub(super) fn retire_process(pid: libc::pid_t) {
    retire_source(SourceKey::Process(pid));
}

/// Retires readiness before the original descriptor closes or transfers.
/// The source itself is moved, never duplicated or retained past close.
#[cfg(unix)]
pub(super) struct OwnedSource<T> {
    // Field order retires the claim before closing the original descriptor.
    owner: DescriptorOwner,
    source: T,
}

#[cfg(unix)]
struct DescriptorOwner(std::os::fd::RawFd);

#[cfg(unix)]
impl Drop for DescriptorOwner {
    fn drop(&mut self) {
        retire_source(SourceKey::Descriptor(self.0));
    }
}

#[cfg(unix)]
impl<T: std::os::fd::AsFd> OwnedSource<T> {
    pub(super) fn new(source: T) -> Self {
        use std::os::fd::AsRawFd as _;
        Self {
            owner: DescriptorOwner(source.as_fd().as_raw_fd()),
            source,
        }
    }

    pub(super) fn into_inner(self) -> T {
        let Self { owner, source } = self;
        drop(owner);
        source
    }
}

#[cfg(unix)]
impl<T: std::os::fd::AsFd> std::os::fd::AsFd for OwnedSource<T> {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.source.as_fd()
    }
}

#[cfg(unix)]
impl<T: std::os::fd::AsFd> std::os::fd::AsRawFd for OwnedSource<T> {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.source.as_fd())
    }
}

#[cfg(unix)]
impl<T: io::Read> io::Read for OwnedSource<T> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.source.read(bytes)
    }
}

#[cfg(unix)]
impl<T: io::Write> io::Write for OwnedSource<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.source.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.source.flush()
    }
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
    registrations: Vec<Registration>,
}

struct Registration {
    #[cfg(unix)]
    source: SourceKey,
    _native: Option<platform::Registration>,
}

/// The progress a nonblocking descriptor operation needs before resuming.
#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
pub enum DescriptorInterest {
    /// Data, end of file or an error is pending.
    Readable,
    /// More bytes can be written, or the peer has closed.
    Writable,
}

/// One descriptor observation served by the shared native event hub.
///
/// The descriptor remains borrowed until this future is dropped. Readiness
/// permits another nonblocking operation; it does not promise that operation
/// will succeed. The original operation still owns its buffers and completion.
#[cfg(unix)]
pub struct DescriptorReadiness<'a> {
    wait: Wait<'static>,
    _descriptor: std::os::fd::BorrowedFd<'a>,
}

/// Arms the shared native event hub after a nonblocking operation would block.
///
/// Each descriptor may belong to only one live readiness observation, including
/// process observations. Competing registration returns `AlreadyExists` before
/// changing native interest. Drop the observation before rearming, including
/// when changing direction. Peer closure wakes it; loss of the shared event
/// producer returns [`ProcessWaitOutcome::ServiceFailed`]. No descriptor is
/// copied and no per-observation thread or timer is created.
#[cfg(unix)]
pub fn descriptor_readiness(
    descriptor: std::os::fd::BorrowedFd<'_>,
    interest: DescriptorInterest,
) -> io::Result<DescriptorReadiness<'_>> {
    let source = match interest {
        DescriptorInterest::Readable => Source::Readable(descriptor),
        DescriptorInterest::Writable => Source::Writable(descriptor),
    };
    Ok(DescriptorReadiness {
        wait: arm(&[source], None)?,
        _descriptor: descriptor,
    })
}

#[cfg(unix)]
impl Future for DescriptorReadiness<'_> {
    type Output = ProcessWaitOutcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.wait).poll(context)
    }
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
    // Construct the owner before taking the lifecycle lock. A partial native
    // registration failure drops that lock before this owner's rollback.
    let mut wait = Wait {
        hub,
        token: 0,
        signal: Arc::new(Signal::default()),
        registrations: Vec::new(),
    };
    let (ready, failure) = {
        let mut registry = hub.registry();
        if let Some(error) = &registry.failure {
            let error = Arc::clone(error);
            drop(registry);
            wait.signal.settle(ProcessWaitOutcome::ServiceFailed(error));
            return Ok(wait);
        }
        registry.next = registry
            .next
            .checked_add(1)
            .filter(|token| *token != u64::MAX)
            .ok_or_else(|| io::Error::other("process readiness tokens exhausted"))?;
        wait.token = registry.next;
        wait.registrations.reserve(sources.len());
        // Registration and producer retirement share this lifecycle lock.
        // No native registration may be dispatched after its producer ends.
        let mut ready = false;
        for source in sources {
            #[cfg(unix)]
            let key = source.key();
            #[cfg(unix)]
            match registry.sources.entry(key) {
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "native readiness source already has a live observation; drop it before rearming",
                    ));
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(wait.token);
                }
            }
            // Retain the claim before the syscall so every refusal releases
            // exactly the sources already admitted to this original wait.
            let index = wait.registrations.len();
            wait.registrations.push(Registration {
                #[cfg(unix)]
                source: key,
                _native: None,
            });
            let native = hub.poller.register(*source, wait.token)?;
            ready = native.is_none();
            wait.registrations
                .get_mut(index)
                .ok_or(io::ErrorKind::InvalidData)?
                ._native = native;
            if ready {
                break;
            }
        }
        let failure = registry.registered(&hub.poller, wait.token, &wait.signal, wake_at, ready);
        (ready, failure)
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
        let mut registry = self.hub.registry();
        registry.remove(self.token);
        #[cfg(unix)]
        for registration in &self.registrations {
            if let std::collections::hash_map::Entry::Occupied(entry) =
                registry.sources.entry(registration.source)
                && entry.get() == &self.token
            {
                entry.remove();
            }
        }
    }
}

/// The number of armed waits the hub holds, for scale tests.
#[cfg(all(test, feature = "test-support", not(loom)))]
pub(super) fn armed_waits() -> usize {
    hub().map_or(0, |hub| hub.registry().armed.len())
}
