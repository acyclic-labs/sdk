//! One inotify instance, router and service pool for every watch in the process.
//!
//! A watch per inotify instance cost each watch a kernel instance (capped per
//! user by `max_user_instances`), a reader thread, a wake descriptor and its own
//! kernel watches on shared ancestors. The hub owns one instance instead. Each
//! watch subscribes the watch descriptors it registers; the router reads the
//! shared queue and appends every record to the inbox of each interested
//! subscriber, then schedules those watches on a small service pool. A watch
//! consumes its inbox exactly as it consumed its own descriptor, so ordering,
//! overflow and fence semantics are unchanged: a fence first drains the shared
//! kernel queue through the router, then consumes its own inbox.
//!
//! Routing work is proportional to the interested subscribers, never to every
//! watch: a watch registers an ancestor only for the one child name on the way
//! to its root, so many watches below one shared directory do not each receive
//! that directory's records. Registration and withdrawal are keyed by the
//! watch's own descriptors, so neither scans the other watches.
//!
//! The kernel allocates watch descriptors cyclically, so a descriptor removed
//! here is not reissued while its trailing `IN_IGNORED` record can still be
//! queued; that record finds no route and is dropped, as the removing watch's
//! own instance would have delivered it to a watch that no longer listed it.
use super::*;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::sync::{OnceLock, Weak};

/// Records a watch may hold undelivered before it reports lost changes.
pub(super) const INBOX_BYTES: usize = READ_BUFFER_BYTES;
/// Threads delivering inbox records to watch sinks.
const SERVICE_THREADS: usize = 2;
/// Records that concern a directory itself rather than one named child.
const SELF_RECORDS: u32 =
    libc::IN_IGNORED | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_UNMOUNT;

/// Which records of one directory a watch receives.
#[derive(Clone, Copy)]
pub(super) enum Interest<'a> {
    /// Every record: a directory the watch reports changes beneath.
    All,
    /// Records about the directory itself or the one named child: an
    /// ancestor on the way to the watch's root.
    Named(&'a OsStr),
}

pub(super) struct Hub {
    descriptor: OwnedFd,
    routes: Mutex<Routes>,
    pending: Mutex<VecDeque<Arc<Shared>>>,
    pending_changed: Condvar,
    next_subscriber: AtomicU64,
}

#[derive(Default)]
struct Route {
    all: HashMap<u64, Weak<Shared>>,
    named: HashMap<OsString, HashMap<u64, Weak<Shared>>>,
}

struct Routes {
    buffer: Vec<u8>,
    /// Watch descriptor -> interested watches.
    descriptors: HashMap<i32, Route>,
    /// Watches that receive queue overflow records.
    subscribers: HashMap<u64, Weak<Shared>>,
}

/// Undelivered records routed to one watch.
#[derive(Default)]
pub(super) struct Inbox {
    bytes: VecDeque<u8>,
    overflowed: bool,
    scheduled: bool,
}

static HUB: OnceLock<Result<Hub, i32>> = OnceLock::new();

pub(super) fn hub() -> Result<&'static Hub, DemandError> {
    HUB.get_or_init(Hub::open)
        .as_ref()
        .map_err(|error| io::Error::from_raw_os_error(*error).into())
}

fn record_length(raw: &[u8]) -> Option<usize> {
    let &[_, _, _, _, _, _, _, _, _, _, _, _, l0, l1, l2, l3] =
        raw.first_chunk::<EVENT_HEADER_BYTES>()?;
    EVENT_HEADER_BYTES.checked_add(u32::from_ne_bytes([l0, l1, l2, l3]) as usize)
}

fn overflow_record() -> [u8; EVENT_HEADER_BYTES] {
    let [d0, d1, d2, d3] = (-1_i32).to_ne_bytes();
    let [m0, m1, m2, m3] = libc::IN_Q_OVERFLOW.to_ne_bytes();
    [d0, d1, d2, d3, m0, m1, m2, m3, 0, 0, 0, 0, 0, 0, 0, 0]
}

impl Hub {
    fn open() -> Result<Self, i32> {
        // SAFETY: plain descriptor creation with valid flags.
        let descriptor = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        let descriptor = owned(descriptor).map_err(|_| {
            io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EMFILE)
        })?;
        Ok(Self {
            descriptor,
            routes: Mutex::new(Routes {
                buffer: vec![0; READ_BUFFER_BYTES],
                descriptors: HashMap::new(),
                subscribers: HashMap::new(),
            }),
            pending: Mutex::new(VecDeque::new()),
            pending_changed: Condvar::new(),
            next_subscriber: AtomicU64::new(1),
        })
    }

    fn start_threads(&'static self) -> io::Result<()> {
        static STARTED: OnceLock<Result<(), i32>> = OnceLock::new();
        STARTED
            .get_or_init(|| {
                let spawn = |name: &str, body: fn(&'static Hub)| {
                    std::thread::Builder::new()
                        .name(name.into())
                        .spawn(move || body(self))
                        .map(drop)
                        .map_err(|error| error.raw_os_error().unwrap_or(libc::EAGAIN))
                };
                spawn("acyclic-watch", Self::route_forever)?;
                for _ in 0..SERVICE_THREADS {
                    spawn("acyclic-watch-service", Self::serve_forever)?;
                }
                Ok(())
            })
            .map_err(io::Error::from_raw_os_error)
    }

    /// Subscribes a new watch to queue overflow records.
    pub(super) fn subscribe(&'static self, watch: &Arc<Shared>) -> Result<(), DemandError> {
        self.start_threads()?;
        self.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .subscribers
            .insert(watch.subscriber, Arc::downgrade(watch));
        Ok(())
    }

    /// Stops routing queue overflow records to `subscriber`.
    pub(super) fn unsubscribe(&self, subscriber: u64) {
        self.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .subscribers
            .remove(&subscriber);
    }

    pub(super) fn next_subscriber(&self) -> u64 {
        self.next_subscriber.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn raw_descriptor(&self) -> i32 {
        self.descriptor.as_raw_fd()
    }

    /// Interests registered on `descriptor`: (all-record, named) counts.
    #[cfg(test)]
    pub(super) fn interests(&self, descriptor: i32) -> (usize, usize) {
        self.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .descriptors
            .get(&descriptor)
            .map_or((0, 0), |route| {
                (
                    route.all.len(),
                    route.named.values().map(HashMap::len).sum(),
                )
            })
    }

    /// Registers `path` for `watch`, merging `mask` into any registration the
    /// same directory already has, and returns its watch descriptor.
    pub(super) fn add(
        &self,
        watch: &Shared,
        path: &std::ffi::CStr,
        mask: u32,
        interest: Interest<'_>,
    ) -> io::Result<i32> {
        let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: a NUL-terminated path that outlives the call.
        let descriptor = unsafe {
            libc::inotify_add_watch(
                self.descriptor.as_raw_fd(),
                path.as_ptr(),
                mask | libc::IN_MASK_ADD,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        let route = routes.descriptors.entry(descriptor).or_default();
        let subscribers = match interest {
            Interest::All => &mut route.all,
            Interest::Named(name) => {
                if !route.named.contains_key(name) {
                    let mut owned = OsString::new();
                    owned
                        .try_reserve_exact(name.len())
                        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
                    owned.push(name);
                    route.named.insert(owned, HashMap::new());
                }
                route
                    .named
                    .get_mut(name)
                    .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?
            }
        };
        subscribers.insert(watch.subscriber, watch.this.clone());
        Ok(descriptor)
    }

    /// Withdraws one registration; the last interest removes the descriptor.
    pub(super) fn remove(
        &self,
        subscriber: u64,
        descriptor: i32,
        interest: Interest<'_>,
    ) -> io::Result<()> {
        let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
        self.remove_locked(&mut routes, subscriber, descriptor, interest)
    }

    fn remove_locked(
        &self,
        routes: &mut Routes,
        subscriber: u64,
        descriptor: i32,
        interest: Interest<'_>,
    ) -> io::Result<()> {
        let Some(route) = routes.descriptors.get_mut(&descriptor) else {
            return Ok(());
        };
        match interest {
            Interest::All => {
                route.all.remove(&subscriber);
            }
            Interest::Named(name) => {
                if let Some(subscribers) = route.named.get_mut(name) {
                    subscribers.remove(&subscriber);
                    if subscribers.is_empty() {
                        route.named.remove(name);
                    }
                }
            }
        }
        if !route.all.is_empty() || !route.named.is_empty() {
            return Ok(());
        }
        routes.descriptors.remove(&descriptor);
        // SAFETY: removes a descriptor no subscriber still uses.
        if unsafe { libc::inotify_rm_watch(self.descriptor.as_raw_fd(), descriptor) } < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINVAL) {
                return Err(error);
            }
        }
        Ok(())
    }

    /// Withdraws every registration a watch lists, and its subscription.
    pub(super) fn withdraw(
        &self,
        subscriber: u64,
        watches: &[(PathBuf, i32)],
        ancestors: &[(i32, OsString)],
    ) -> io::Result<()> {
        let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
        routes.subscribers.remove(&subscriber);
        let mut result = Ok(());
        for (_, descriptor) in watches {
            let removed = self.remove_locked(&mut routes, subscriber, *descriptor, Interest::All);
            result = result.and(removed);
        }
        for (descriptor, name) in ancestors {
            let removed =
                self.remove_locked(&mut routes, subscriber, *descriptor, Interest::Named(name));
            result = result.and(removed);
        }
        result
    }

    /// Routes every record the kernel has queued, then schedules the watches
    /// that received records once the routing lock is released.
    pub(super) fn drain(&self) -> io::Result<()> {
        let mut ready = Vec::new();
        let result = {
            let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
            self.drain_locked(&mut routes, &mut ready)
        };
        self.schedule_ready(ready);
        result
    }

    /// Schedules the watches that became due. Every watch upgraded under the
    /// routing lock is released only here, after that lock, so a watch's
    /// last reference never drops while the hub's tables are held.
    fn schedule_ready(&self, ready: Vec<(Arc<Shared>, bool)>) {
        for (watch, due) in ready {
            if due {
                self.schedule(watch);
            }
        }
    }

    fn drain_locked(&self, routes: &mut Routes, ready: &mut Ready) -> io::Result<()> {
        loop {
            let length = routes.buffer.len();
            // SAFETY: the routing lock owns this buffer for the read.
            let read = unsafe {
                libc::read(
                    self.descriptor.as_raw_fd(),
                    routes.buffer.as_mut_ptr().cast(),
                    length,
                )
            };
            if read < 0 {
                let error = io::Error::last_os_error();
                match error.kind() {
                    io::ErrorKind::WouldBlock => return Ok(()),
                    io::ErrorKind::Interrupted => continue,
                    _ => return Err(error),
                }
            }
            let filled =
                usize::try_from(read).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
            if filled == 0 {
                return Ok(());
            }
            let Routes {
                buffer,
                descriptors,
                subscribers,
            } = routes;
            let mut cursor = 0;
            while let Some(raw) = buffer.get(cursor..filled).filter(|raw| !raw.is_empty()) {
                let Some(record) = record_length(raw).and_then(|end| raw.get(..end)) else {
                    // A malformed read loses records: every watch must rescan.
                    for watch in subscribers.values().filter_map(Weak::upgrade) {
                        deliver_overflow(watch, ready);
                    }
                    break;
                };
                cursor += record.len();
                let (Some(&[d0, d1, d2, d3, m0, m1, m2, m3]), Some(name)) =
                    (record.first_chunk::<8>(), record.get(EVENT_HEADER_BYTES..))
                else {
                    continue;
                };
                let descriptor = i32::from_ne_bytes([d0, d1, d2, d3]);
                let mask = u32::from_ne_bytes([m0, m1, m2, m3]);
                let name = OsStr::from_bytes(name.split(|byte| *byte == 0).next().unwrap_or(&[]));
                // Only queue overflow concerns every watch; an unmount names
                // the descriptor of the watched directory it removed.
                if descriptor < 0 || mask & libc::IN_Q_OVERFLOW != 0 {
                    for watch in subscribers.values().filter_map(Weak::upgrade) {
                        deliver(watch, record, ready);
                    }
                    continue;
                }
                let Some(route) = descriptors.get(&descriptor) else {
                    continue;
                };
                for watch in route.all.values().filter_map(Weak::upgrade) {
                    deliver(watch, record, ready);
                }
                let named = if name.is_empty() || mask & SELF_RECORDS != 0 {
                    // Records about the directory itself reach every interest.
                    route
                        .named
                        .values()
                        .flat_map(HashMap::iter)
                        .collect::<Vec<_>>()
                } else {
                    route.named.get(name).into_iter().flatten().collect()
                };
                for (subscriber, watch) in named {
                    if route.all.contains_key(subscriber) {
                        continue;
                    }
                    if let Some(watch) = watch.upgrade() {
                        deliver(watch, record, ready);
                    }
                }
                if mask & libc::IN_IGNORED != 0 {
                    // The kernel removed this descriptor.
                    descriptors.remove(&descriptor);
                }
            }
        }
    }

    pub(super) fn schedule(&self, watch: Arc<Shared>) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(watch);
        self.pending_changed.notify_one();
    }

    fn route_forever(&'static self) {
        let mut descriptor = libc::pollfd {
            fd: self.descriptor.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            // SAFETY: one initialized pollfd record.
            let ready = unsafe { libc::poll(&raw mut descriptor, 1, -1) };
            if ready < 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                self.fail_all();
                return;
            }
            if self.drain().is_err() {
                self.fail_all();
                return;
            }
        }
    }

    /// The shared queue failed: no watch can observe changes exactly again.
    fn fail_all(&self) {
        let mut ready = Vec::new();
        {
            let routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
            for watch in routes.subscribers.values().filter_map(Weak::upgrade) {
                deliver_overflow(watch, &mut ready);
            }
        }
        self.schedule_ready(ready);
    }

    fn serve_forever(&'static self) {
        loop {
            let watch = {
                let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
                loop {
                    if let Some(watch) = pending.pop_front() {
                        break watch;
                    }
                    pending = self
                        .pending_changed
                        .wait(pending)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            };
            watch.service();
        }
    }
}

/// Watches that received records, and whether each needs a service pass.
type Ready = Vec<(Arc<Shared>, bool)>;

/// Appends one record; a full inbox records loss instead.
fn deliver(watch: Arc<Shared>, record: &[u8], ready: &mut Ready) {
    let due = {
        let mut inbox = watch.inbox.lock().unwrap_or_else(PoisonError::into_inner);
        if !inbox.overflowed {
            if inbox.bytes.len() + record.len() > INBOX_BYTES
                || inbox.bytes.try_reserve(record.len()).is_err()
            {
                inbox.overflowed = true;
            } else {
                inbox.bytes.extend(record);
            }
        }
        !std::mem::replace(&mut inbox.scheduled, true)
    };
    ready.push((watch, due));
}

fn deliver_overflow(watch: Arc<Shared>, ready: &mut Ready) {
    let due = {
        let mut inbox = watch.inbox.lock().unwrap_or_else(PoisonError::into_inner);
        inbox.overflowed = true;
        !std::mem::replace(&mut inbox.scheduled, true)
    };
    ready.push((watch, due));
}

impl Inbox {
    /// Moves whole records, at most `maximum` bytes, into `buffer`. An inbox
    /// that lost records yields one queue-overflow record once drained.
    pub(super) fn take(&mut self, buffer: &mut Vec<u8>, maximum: usize) -> io::Result<usize> {
        buffer.clear();
        while self.bytes.len() >= EVENT_HEADER_BYTES {
            let mut header = [0_u8; EVENT_HEADER_BYTES];
            for (target, byte) in header.iter_mut().zip(self.bytes.iter()) {
                *target = *byte;
            }
            let length = record_length(&header)
                .filter(|length| *length <= self.bytes.len())
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
            if buffer.len() + length > maximum {
                if buffer.is_empty() {
                    // inotify reports a buffer too small for the next record.
                    return Err(io::Error::from_raw_os_error(libc::EINVAL));
                }
                break;
            }
            buffer
                .try_reserve(length)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
            buffer.extend(self.bytes.drain(..length));
        }
        if self.bytes.is_empty() {
            self.bytes = VecDeque::new();
            if self.overflowed && buffer.len() + EVENT_HEADER_BYTES <= maximum {
                self.overflowed = false;
                buffer.extend_from_slice(&overflow_record());
            }
        }
        if buffer.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        Ok(buffer.len())
    }

    /// Routed bytes waiting and whether an overflow is pending.
    #[cfg(test)]
    pub(super) fn pending(&self) -> (usize, bool) {
        (self.bytes.len(), self.overflowed)
    }

    /// Records that a service pass started: later records schedule again.
    pub(super) fn begin_service(&mut self) {
        self.scheduled = false;
    }

    /// Marks the inbox scheduled; returns whether it holds records and was not
    /// already scheduled.
    pub(super) fn reschedule(&mut self) -> bool {
        let due = !self.scheduled && (!self.bytes.is_empty() || self.overflowed);
        self.scheduled |= due;
        due
    }
}
