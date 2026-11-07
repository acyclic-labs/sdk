//! Native readiness producers shared by every process wait.
//!
//! A waiting child holds no thread. Each wait arms one-shot interest in its
//! pipes, its exit and an optional wake time with fixed producers, one per
//! available CPU (epoll with pidfds on Linux, kqueue with `EVFILT_PROC` on
//! macOS, an I/O completion port with pipe operations and registered process
//! waits on Windows), and
//! the first of them to fire wakes the waiter. An async waiter costs a waker;
//! a synchronous caller parks its own thread.

use std::collections::HashMap;
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

mod deadlines;
mod outcome;
mod pool;
mod signal;
pub use outcome::ProcessWaitOutcome;
use pool::pool;
use signal::Signal;

fn copy_error(error: &io::Error) -> io::Error {
    error.raw_os_error().map_or_else(
        || io::Error::new(error.kind(), error.to_string()),
        io::Error::from_raw_os_error,
    )
}

struct Armed {
    signal: Arc<Signal>,
    deadline: Option<deadlines::Key>,
}

#[derive(Default)]
struct Registry {
    next: usize,
    // A dense live list makes terminal notification proportional only to the
    // waiters it must settle, independent of cancelled/historical entries.
    armed: Vec<(usize, Armed)>,
    positions: HashMap<usize, usize>,
    #[cfg(unix)]
    sources: HashMap<SourceKey, usize>,
    timers: deadlines::Deadlines,
    failure: Option<Arc<io::Error>>,
}

impl Registry {
    fn registered(
        &mut self,
        poller: &platform::Poller,
        token: usize,
        signal: &Arc<Signal>,
        wake_at: Option<Instant>,
        ready: bool,
    ) -> io::Result<Option<Failure>> {
        if ready {
            return Ok(None);
        }
        let earliest =
            wake_at.is_some_and(|at| self.timers.first().is_none_or(|(first, _)| at < first));
        let deadline = wake_at
            .map(|at| self.timers.insert(at, token))
            .transpose()?;
        self.positions.insert(token, self.armed.len());
        self.armed.push((
            token,
            Armed {
                signal: Arc::clone(signal),
                deadline,
            },
        ));
        Ok(earliest
            .then(|| poller.notify())
            .and_then(Result::err)
            .and_then(|error| self.fail(error)))
    }

    fn remove(&mut self, token: usize) -> Option<Armed> {
        let position = self.positions.remove(&token)?;
        let (_, armed) = self.armed.swap_remove(position);
        if let Some((moved, _)) = self.armed.get(position) {
            self.positions.insert(*moved, position);
        }
        if let Some(key) = armed.deadline {
            self.timers.remove(key);
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
    armed: Vec<(usize, Armed)>,
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
    stride: usize,
    service_failure: Arc<OnceLock<Arc<io::Error>>>,
    #[cfg(test)]
    locks: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
static STARTED: AtomicBool = AtomicBool::new(false);

/// Whether anything in this process has started the readiness service.
#[cfg(test)]
fn started() -> bool {
    STARTED.load(Ordering::Acquire)
}

/// The original readiness service is started before creating a child.
/// A partial startup refusal ends its waiting producers and fails the spawn.
#[derive(Clone, Copy)]
pub(super) struct Events {
    _pool: &'static pool::Pool,
}

impl Events {
    pub(super) fn start() -> io::Result<Self> {
        let pool = pool()?;
        pool.check()?;
        Ok(Self { _pool: pool })
    }
}

// Source retirement precedes physical close or ownership transfer. An old
// wait may outlive that source, but must not reserve a reused descriptor/PID.
#[cfg(unix)]
fn retire_source(source: SourceKey) {
    let Some(pool) = pool::existing() else {
        return;
    };
    let hub = pool.source(source);
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
        #[cfg(test)]
        self.locks.fetch_add(1, Ordering::Relaxed);
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn fail(&self, error: io::Error) {
        let failure = self.registry().fail(error);
        if let Some(failure) = failure {
            self.service_failure
                .get_or_init(|| Arc::clone(&failure.error));
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

    fn dispatch(&self, ready: &mut Vec<usize>, now: Instant) {
        let mut fired = Vec::new();
        {
            let mut registry = self.registry();
            for token in ready.drain(..) {
                if let Some(armed) = registry.remove(token) {
                    fired.push((armed.signal, ProcessWaitOutcome::Ready));
                }
            }
            while let Some((at, token)) = registry.timers.first() {
                if at > now {
                    break;
                }
                if let Some(armed) = registry.remove(token) {
                    fired.push((armed.signal, ProcessWaitOutcome::Deadline));
                } else {
                    unreachable!("a deadline retains its original armed registration");
                }
            }
        }
        deliver(fired.into_iter());
    }
}

fn deliver(signals: impl Iterator<Item = (Arc<Signal>, ProcessWaitOutcome)>) {
    crate::delivery::deliver(signals, |(signal, outcome)| {
        signal.settle(outcome);
    });
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
    signal: Arc<Signal>,
    // The common single-source/deadline wait needs no registration allocation.
    first: Option<Registration<'a>>,
    other: Vec<Registration<'a>>,
}

struct Registration<'a> {
    hub: &'a Hub,
    token: usize,
    #[cfg(unix)]
    source: Option<SourceKey>,
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

/// One descriptor observation served by its original native producer.
///
/// The descriptor remains borrowed until this future is dropped. Readiness
/// permits another nonblocking operation; it does not promise that operation
/// will succeed. The original operation still owns its buffers and completion.
#[cfg(unix)]
pub struct DescriptorReadiness<'a> {
    wait: Wait<'static>,
    _descriptor: std::os::fd::BorrowedFd<'a>,
}

/// Arms the original native producer after a nonblocking operation would block.
///
/// Each descriptor may belong to only one live readiness observation, including
/// process observations. Competing registration returns `AlreadyExists` before
/// changing native interest. Drop the observation before rearming, including
/// when changing direction. Peer closure wakes it; loss of its original event
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
    pool()?.arm(sources, wake_at)
}

#[cfg(test)]
fn arm_on<'a>(
    hub: &'a Hub,
    sources: &[Source<'_>],
    wake_at: Option<Instant>,
) -> io::Result<Wait<'a>> {
    arm_with(sources, wake_at, |_| hub)
}

fn arm_with<'a>(
    sources: &[Source<'_>],
    mut wake_at: Option<Instant>,
    owner: impl Fn(Option<Source<'_>>) -> &'a Hub,
) -> io::Result<Wait<'a>> {
    #[cfg(all(test, feature = "test-support", not(loom)))]
    LIVE_WAITS.fetch_add(1, Ordering::Relaxed);
    let mut wait = Wait {
        signal: Arc::new(Signal::default()),
        first: None,
        other: Vec::with_capacity(sources.len().saturating_sub(1)),
    };
    if sources.is_empty() {
        wait.register(owner(None), None, wake_at)?;
    } else {
        for source in sources {
            if wait.register(owner(Some(*source)), Some(*source), wake_at.take())? {
                break;
            }
        }
    }
    Ok(wait)
}

impl<'a> Wait<'a> {
    fn register(
        &mut self,
        hub: &'a Hub,
        source: Option<Source<'_>>,
        wake_at: Option<Instant>,
    ) -> io::Result<bool> {
        // The original entry owns rollback before native admission. Its Drop
        // runs after the lifecycle lock, including on a partially refused arm.
        let mut entry = Registration {
            hub,
            token: 0,
            #[cfg(unix)]
            source: None,
            _native: None,
        };
        let (ready, failure) = {
            let mut registry = hub.registry();
            if let Some(error) = &registry.failure {
                let error = Arc::clone(error);
                drop(registry);
                self.signal.settle(ProcessWaitOutcome::ServiceFailed(error));
                return Ok(true);
            }
            registry.next = registry
                .next
                .checked_add(hub.stride)
                .filter(|token| *token != usize::MAX)
                .ok_or_else(|| io::Error::other("process readiness tokens exhausted"))?;
            entry.token = registry.next;
            let mut ready = false;
            if let Some(source) = source {
                #[cfg(unix)]
                match registry.sources.entry(source.key()) {
                    std::collections::hash_map::Entry::Occupied(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "native readiness source already has a live observation; drop it before rearming",
                        ));
                    }
                    std::collections::hash_map::Entry::Vacant(claim) => {
                        claim.insert(entry.token);
                        entry.source = Some(source.key());
                    }
                }
                entry._native = hub.poller.register(source, entry.token)?;
                ready = entry._native.is_none();
            }
            let failure =
                registry.registered(&hub.poller, entry.token, &self.signal, wake_at, ready)?;
            (ready, failure)
        };
        if self.first.is_none() {
            self.first = Some(entry);
        } else {
            self.other.push(entry);
        }
        // Wakers may re-enter admission. Always deliver outside native locks.
        if let Some(failure) = failure {
            hub.service_failure
                .get_or_init(|| Arc::clone(&failure.error));
            failure.deliver();
            Ok(true)
        } else {
            if ready {
                self.signal.settle(ProcessWaitOutcome::Ready);
            }
            Ok(ready)
        }
    }
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

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        if self.token == 0 {
            return;
        }
        let mut registry = self.hub.registry();
        registry.remove(self.token);
        #[cfg(unix)]
        if let Some(source) = self.source
            && let std::collections::hash_map::Entry::Occupied(entry) =
                registry.sources.entry(source)
            && entry.get() == &self.token
        {
            entry.remove();
        }
        // The guard drops before the native field's destructor. A late native
        // callback can only post its original token, never a borrowed entry.
    }
}

#[cfg(all(test, feature = "test-support", not(loom)))]
static LIVE_WAITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(all(test, feature = "test-support", not(loom)))]
impl Drop for Wait<'_> {
    fn drop(&mut self) {
        LIVE_WAITS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Counts original wait owners, independent of their number of native sources.
#[cfg(all(test, feature = "test-support", not(loom)))]
pub(super) fn live_waits() -> usize {
    LIVE_WAITS.load(Ordering::Relaxed)
}
