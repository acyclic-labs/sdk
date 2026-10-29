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
//! A service pass never waits for a watch's read cursor (see `Reading`):
//! with the pool shared, a pass parked behind one watch's fence or pause
//! would stall delivery to every watch in the process.
//!
//! Routing work is proportional to the interested subscribers, never to every
//! watch: a watch registers an ancestor only for the one child name on the way
//! to its root, so many watches below one shared directory do not each receive
//! that directory's records. Registration and withdrawal are keyed by the
//! watch's own descriptors, so neither scans the other watches. A registration
//! first routes the records already queued, so a watch never receives records
//! that predate it, as with an instance of its own.
//!
//! The kernel allocates watch descriptors cyclically, so a descriptor removed
//! here is not reissued while its trailing `IN_IGNORED` record can still be
//! queued; that record finds no route and is dropped, as the removing watch's
//! own instance would have delivered it to a watch that no longer listed it.
use super::*;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::sync::{OnceLock, Weak};

/// Threads delivering inbox records to watch sinks.
pub(super) const SERVICE_THREADS: usize = 2;
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
    failure: AtomicI32,
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
    failure: Option<DemandError>,
    scheduled: bool,
}

static HUB: OnceLock<Hub> = OnceLock::new();

/// Held by a test to keep the router from reading, so records stay queued.
#[cfg(test)]
pub(super) static ROUTER_PAUSED: Mutex<()> = Mutex::new(());

pub(super) fn hub() -> Result<&'static Hub, DemandError> {
    if let Some(hub) = HUB.get() {
        return Ok(hub);
    }
    // A failed open (a descriptor or memory limit) is retried by the next
    // watch; a racing opener's instance closes when its value drops.
    let opened = Hub::open().map_err(io::Error::from_raw_os_error)?;
    Ok(HUB.get_or_init(|| opened))
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
            failure: AtomicI32::new(0),
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

    /// Starts the router and the service pool once. A failed spawn is
    /// retried by the next watch without duplicating threads already running.
    fn start_threads(&'static self) -> io::Result<()> {
        static STARTED: Mutex<usize> = Mutex::new(0);
        let mut started = STARTED.lock().unwrap_or_else(PoisonError::into_inner);
        while *started <= SERVICE_THREADS {
            let (name, body): (&str, fn(&'static Self)) = if *started == 0 {
                ("acyclic-watch", Self::route_forever)
            } else {
                ("acyclic-watch-service", Self::serve_forever)
            };
            // A spawn can return before the new thread has set its OS name.
            // Tests census the physical pool, so observe entry before making
            // that pool visible to their first subscription.
            #[cfg(test)]
            let (ready, entered) = std::sync::mpsc::channel();
            std::thread::Builder::new()
                .name(name.into())
                .spawn(move || {
                    #[cfg(test)]
                    let _ = ready.send(());
                    body(self);
                })?;
            *started += 1;
            #[cfg(test)]
            entered.recv().map_err(io::Error::other)?;
        }
        Ok(())
    }

    /// Subscribes a new watch to queue overflow records.
    pub(super) fn subscribe(&'static self, watch: &Arc<Shared>) -> Result<(), DemandError> {
        self.check()?;
        self.start_threads()?;
        let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
        self.check()?;
        routes
            .subscribers
            .insert(watch.subscriber, Arc::downgrade(watch));
        Ok(())
    }

    pub(super) fn check(&self) -> io::Result<()> {
        match self.failure.load(Ordering::Acquire) {
            0 => Ok(()),
            code => Err(io::Error::from_raw_os_error(code)),
        }
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
        let mut ready = Vec::new();
        let added = {
            let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
            // Records already queued predate this registration: a directory
            // other watches share may hold records about this watch's names
            // from before it existed. Route them to the earlier interests
            // first, as a fresh instance would never have queued them.
            self.check()
                .and_then(|()| self.drain_locked(&mut routes, &mut ready))
                .and_then(|()| self.add_locked(&mut routes, watch, path, mask, interest))
        };
        self.schedule_ready(ready);
        added
    }

    fn add_locked(
        &self,
        routes: &mut Routes,
        watch: &Shared,
        path: &std::ffi::CStr,
        mask: u32,
        interest: Interest<'_>,
    ) -> io::Result<i32> {
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

    /// Bytes the kernel has queued.
    fn queued(&self) -> io::Result<usize> {
        let mut queued: libc::c_int = 0;
        // SAFETY: FIONREAD writes one int, the queued byte count.
        if unsafe { libc::ioctl(self.descriptor.as_raw_fd(), libc::FIONREAD, &raw mut queued) } < 0
        {
            return Err(io::Error::last_os_error());
        }
        usize::try_from(queued).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
    }

    /// Routes every record the kernel queued before this call, so a fence
    /// observes them in its inbox. Records arriving meanwhile are left to the
    /// router: the work is bounded by the queue at entry, however busy.
    pub(super) fn drain(&self) -> io::Result<()> {
        self.check()?;
        let queued = self.queued()?;
        // The queue is FIFO and the router reads under the routing lock: once
        // this call has read `queued` bytes or found the queue empty, every
        // record queued at entry was routed by this call or before its batch.
        let mut consumed = 0_usize;
        loop {
            let read = self.drain_batch()?;
            consumed = consumed.saturating_add(read);
            if read == 0 || consumed >= queued {
                return Ok(());
            }
        }
    }

    /// `drain` under one hold of the routing lock.
    fn drain_locked(&self, routes: &mut Routes, ready: &mut Ready) -> io::Result<()> {
        let queued = self.queued()?;
        let mut consumed = 0_usize;
        loop {
            let read = self.route_batch(routes, ready)?;
            consumed = consumed.saturating_add(read);
            if read == 0 || consumed >= queued {
                return Ok(());
            }
        }
    }

    /// Reads and routes one buffer of records, then schedules the watches
    /// that received them once the routing lock is released. Returns the
    /// bytes read; zero when the queue is empty.
    fn drain_batch(&self) -> io::Result<usize> {
        let mut ready = Vec::new();
        let result = {
            let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
            self.route_batch(&mut routes, &mut ready)
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

    fn route_batch(&self, routes: &mut Routes, ready: &mut Ready) -> io::Result<usize> {
        self.check()?;
        let filled = loop {
            let length = routes.buffer.len();
            // SAFETY: the routing lock owns this buffer for the read.
            let read = unsafe {
                libc::read(
                    self.descriptor.as_raw_fd(),
                    routes.buffer.as_mut_ptr().cast(),
                    length,
                )
            };
            if read >= 0 {
                break usize::try_from(read)
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::WouldBlock => return Ok(0),
                io::ErrorKind::Interrupted => {}
                _ => return Err(error),
            }
        };
        {
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
        Ok(filled)
    }

    pub(super) fn schedule(&self, watch: Arc<Shared>) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(watch);
        self.pending_changed.notify_one();
    }

    fn route_forever(&'static self) {
        self.route_descriptor(self.descriptor.as_raw_fd());
    }

    fn route_descriptor(&self, source: i32) {
        let mut descriptor = libc::pollfd {
            fd: source,
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            // SAFETY: one initialized pollfd record.
            let ready = unsafe { libc::poll(&raw mut descriptor, 1, -1) };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                self.fail_all(error);
                return;
            }
            if descriptor.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                let code = if descriptor.revents & libc::POLLNVAL != 0 {
                    libc::EBADF
                } else {
                    libc::EIO
                };
                self.fail_all(io::Error::from_raw_os_error(code));
                return;
            }
            #[cfg(test)]
            let paused = ROUTER_PAUSED.lock().unwrap_or_else(PoisonError::into_inner);
            // One buffer per wakeup: delivery is scheduled between batches.
            let routed = self.drain_batch();
            #[cfg(test)]
            drop(paused);
            if let Err(error) = routed {
                self.fail_all(error);
                return;
            }
        }
    }

    /// Terminal source loss is distinct from a recoverable queue overflow.
    fn fail_all(&self, error: io::Error) {
        let code = error.raw_os_error().unwrap_or(libc::EIO);
        let watches = {
            let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
            if self
                .failure
                .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
            std::mem::take(&mut routes.subscribers)
        };
        for watch in watches.into_values().filter_map(|watch| watch.upgrade()) {
            // Mark a pending terminal pass so a held cursor reschedules it on
            // release. Notification itself never waits for that cursor.
            let error = watch
                .inbox
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .failure
                .replace(io::Error::from_raw_os_error(code).into())
                .unwrap_or_else(|| io::Error::from_raw_os_error(code).into());
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut work = watch.delivery.callback_work();
                watch.delivery.failed_with_work(error, &mut work);
            }));
            self.schedule(watch);
        }
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

/// Appends one record using the watch's original allocation owner.
fn deliver(watch: Arc<Shared>, record: &[u8], ready: &mut Ready) {
    let due = {
        let mut inbox = watch.inbox.lock().unwrap_or_else(PoisonError::into_inner);
        if !inbox.overflowed && inbox.failure.is_none() {
            let mut work = watch.delivery.callback_work();
            if let Err(error) = inbox.append(record, &watch, &mut work) {
                inbox.failure = Some(error);
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
    fn append<W: SourceDemandReadWork>(
        &mut self,
        record: &[u8],
        watch: &Shared,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.admit(copied(record.len() as u64))?;
        let needed = self
            .bytes
            .len()
            .checked_add(record.len())
            .ok_or(WorkError::Overflow)?;
        let old = self.bytes.capacity();
        if needed > old {
            let capacity = old.checked_mul(2).ok_or(WorkError::Overflow)?.max(needed);
            watch
                .ownership
                .claim_retained(capacity as u64, WorkCounters::UNCHARGED, work)?;
            let mut grown = VecDeque::new();
            if let Err(error) = grown.try_reserve_exact(capacity) {
                watch.ownership.discard_retained(capacity as u64, work)?;
                return Err(DemandError::Allocation(error));
            }
            let actual = grown.capacity();
            // Reconcile allocator rounding before exposing or copying payload.
            if actual > capacity {
                if let Err(error) = watch.ownership.claim_retained(
                    (actual - capacity) as u64,
                    WorkCounters::UNCHARGED,
                    work,
                ) {
                    drop(grown);
                    watch.ownership.discard_retained(capacity as u64, work)?;
                    return Err(error);
                }
            }
            if let Err(error) = work.started_io(copied(self.bytes.len() as u64)) {
                drop(grown);
                watch.ownership.discard_retained(actual as u64, work)?;
                return Err(error);
            }
            if watch
                .retained_variable_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
                    bytes.checked_add((actual - old) as u64)
                })
                .is_err()
            {
                drop(grown);
                watch.ownership.discard_retained(actual as u64, work)?;
                return Err(WorkError::Overflow.into());
            }
            grown.extend(self.bytes.drain(..));
            self.bytes = grown;
            watch.ownership.retire(old as u64)?;
            if !watch.ownership.has_native_custody() {
                work.release(actual as u64)?;
            }
        }
        work.started_io(copied(record.len() as u64))?;
        self.bytes.extend(record);
        Ok(())
    }

    /// Moves whole records, at most `maximum` bytes, into `buffer`. An inbox
    /// that lost records yields one queue-overflow record once drained.
    pub(super) fn take<W: SourceDemandReadWork>(
        &mut self,
        buffer: &mut Vec<u8>,
        maximum: usize,
        watch: &Shared,
        work: &mut W,
    ) -> Result<usize, DemandError> {
        buffer.clear();
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        if !self.bytes.is_empty() || self.overflowed {
            buffer
                .try_reserve_exact(maximum)
                .map_err(DemandError::Allocation)?;
        }
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
                    return Err(io::Error::from_raw_os_error(libc::EINVAL).into());
                }
                break;
            }
            work.started_io(copied(length as u64))?;
            buffer.extend(self.bytes.drain(..length));
        }
        if self.bytes.is_empty() {
            let bytes = self.bytes.capacity() as u64;
            self.bytes = VecDeque::new();
            watch
                .retained_variable_bytes
                .fetch_sub(bytes, Ordering::AcqRel);
            watch.ownership.retire(bytes)?;
            if self.overflowed && buffer.len() + EVENT_HEADER_BYTES <= maximum {
                self.overflowed = false;
                buffer.extend_from_slice(&overflow_record());
            }
        }
        if buffer.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock).into());
        }
        Ok(buffer.len())
    }

    /// Routed bytes waiting and whether an overflow is pending.
    #[cfg(test)]
    pub(super) fn pending(&self) -> (usize, bool) {
        (self.bytes.len(), self.overflowed)
    }

    #[cfg(test)]
    pub(super) fn failure(&self) -> Option<&DemandError> {
        self.failure.as_ref()
    }

    /// Whether a service pass is scheduled and has not yet started.
    #[cfg(test)]
    pub(super) fn scheduled(&self) -> bool {
        self.scheduled
    }

    /// Records that a service pass started: later records schedule again.
    pub(super) fn begin_service(&mut self) {
        self.scheduled = false;
    }

    /// Marks the inbox scheduled; returns whether it holds records and was not
    /// already scheduled.
    pub(super) fn reschedule(&mut self) -> bool {
        let due = !self.scheduled
            && (!self.bytes.is_empty() || self.overflowed || self.failure.is_some());
        self.scheduled |= due;
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_watch::HostChangeSink;
    use std::sync::mpsc;
    use std::time::Duration;

    struct Lost(mpsc::Sender<()>);

    impl HostChangeSink for Lost {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }

        fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
            work.check()
        }

        fn delivery_lost(&self) {
            let _ = self.0.send(());
        }
    }

    fn isolate_router_failure() -> io::Result<bool> {
        const CHILD: &str = "ACYCLIC_TEST_INOTIFY_ROUTER_FAILURE";
        if std::env::var_os(CHILD).is_none() {
            // Terminal health belongs to the process-wide service. Isolate the
            // actual service failure from every other real-watch control.
            let output = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "source_watch::inotify::hub::tests::fatal_router_loss_notifies_held_watches_and_refuses_future_admission",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return Ok(true);
        }
        Ok(false)
    }

    #[test]
    fn fatal_router_loss_notifies_held_watches_and_refuses_future_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        if isolate_router_failure()? {
            return Ok(());
        }

        let root = tempfile::tempdir()?;
        let mut work = crate::native_host::ordinary_work();
        let host = crate::native_host::HostRoot::open(root.path(), &mut work)?;
        let (lost, observed) = mpsc::channel();
        let sink = Arc::new(Lost(lost));
        let watches = (0..2)
            .map(|_| {
                let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
                NativeSourceWatch::start_driver(&host, delivery, &mut work)
            })
            .collect::<Result<Vec<_>, DemandError>>()?;
        let held = watches
            .iter()
            .map(|watch| watch.shared.reading())
            .collect::<Vec<_>>();
        let hub = hub()?;
        assert!(hub.check().is_ok());

        let mut pipe = [-1; 2];
        // SAFETY: two initialized descriptor slots, adopted once on success.
        if unsafe { libc::pipe2(pipe.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let reader = owned(pipe[0])?;
        let writer = owned(pipe[1])?;
        drop(writer);
        // Real POLLHUP is a terminal producer loss, even though poll itself
        // succeeds. The original router wait must return instead of spinning.
        let (finished, returned) = mpsc::channel();
        let router = std::thread::spawn(move || {
            hub.route_descriptor(reader.as_raw_fd());
            let _ = finished.send(());
        });
        returned.recv_timeout(Duration::from_secs(2))?;
        router.join().expect("native router");
        for watch in &watches {
            observed.recv_timeout(Duration::from_secs(2))?;
            assert!(!watch.delivery().is_exact());
            let shared = Arc::clone(&watch.shared);
            let (finished, returned) = mpsc::channel();
            let observer = std::thread::spawn(move || {
                let mut work = crate::native_host::ordinary_work();
                let refused = (0..2).all(|_| {
                    matches!(
                        shared.read_events_and_deliver(Duration::MAX, &mut work),
                        Err(DemandError::Io(error)) if error.raw_os_error() == Some(libc::EIO)
                    )
                });
                let _ = finished.send(refused);
            });
            assert!(returned.recv_timeout(Duration::from_secs(2))?);
            observer.join().expect("native observer");
        }
        hub.fail_all(io::Error::from_raw_os_error(libc::EBADF));
        assert_eq!(
            hub.check().expect_err("terminal health").raw_os_error(),
            Some(libc::EIO)
        );
        assert!(matches!(
            observed.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let path = CString::new(root.path().as_os_str().as_bytes())?;
        assert_eq!(
            hub.add(&watches[0].shared, &path, WATCHED, Interest::All)
                .expect_err("no registration after producer loss")
                .raw_os_error(),
            Some(libc::EIO)
        );
        let delivery = Arc::new(Delivery::new(sink, &mut work)?);
        assert!(matches!(
            NativeSourceWatch::start_driver(&host, delivery, &mut work),
            Err(DemandError::Io(error)) if error.raw_os_error() == Some(libc::EIO)
        ));
        drop(held);
        for watch in watches {
            watch.stop_with_work(&mut work)?;
        }
        Ok(())
    }
}
