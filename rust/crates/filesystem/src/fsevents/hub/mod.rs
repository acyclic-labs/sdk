//! The process's few shared `FSEvents` streams, which every [`EventStream`]
//! registers its roots on.
//!
//! A process can run only about 512 streams, so one stream per watch cannot
//! reach the number of watches the harness keeps. The hub runs at most
//! [`SHARDS`], made as they are first needed, all on one serial queue. Each
//! registration joins the least-loaded shard, and each event reaches the
//! registrations whose root is its path or a component-boundary ancestor.
//!
//! A shard whose roots grow is re-armed: its new stream replays history from
//! the older of the last event the shard delivered and each new
//! registration's start id. Every route drops an event at or below its
//! floor, the last id its registration has had or did not want, so a re-arm
//! neither loses nor repeats an event. History arrives in path order, so a
//! shard re-arms only once its replay is done. Re-arming suspends the queue
//! rather than waiting for it, so a blocked handler delays no registration
//! and a handler may register or stop watches. Unregistering only removes routes;
//! the stream forgets their roots at its next re-arm, which comes at once
//! when more of its roots are dead than live.

use super::*;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{MutexGuard, PoisonError};

#[cfg(test)]
mod tests;

/// The start id of a registration not yet armed: the id when it is.
const AT_ARM: u64 = EVENT_ID_SINCE_NOW;

/// The most native streams the hub runs.
pub(super) const SHARDS: usize = 16;

static HUB: OnceLock<Hub> = OnceLock::new();

/// One watch's roots and handler, shared by its routes and its owner.
pub(super) struct Registration {
    roots: Vec<PathBuf>,
    handler: Arc<BorrowedEventHandlerFn>,
    live: AtomicBool,
    routed: AtomicBool,
    failed: AtomicBool,
    shard: AtomicUsize,
    // A failed arm writes this before queue submission under routing custody.
    // The serial queue consumes it once, before terminal cleanup can pass it.
    failure: UnsafeCell<Option<Arc<DemandError>>>,
    // Only the hub's serial queue accesses this slot.
    pub(super) terminal: UnsafeCell<Option<EventStream>>,
}

// SAFETY: handlers are Send+Sync; the terminal slot has a single queue-local
// writer/consumer.
unsafe impl Send for Registration {}
// SAFETY: shared callers never access the queue-local terminal slot concurrently.
unsafe impl Sync for Registration {}

impl Registration {
    pub(super) fn new(roots: Vec<PathBuf>, handler: Arc<BorrowedEventHandlerFn>) -> Self {
        Self {
            roots,
            handler,
            live: AtomicBool::new(true),
            routed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            shard: AtomicUsize::new(0),
            terminal: UnsafeCell::new(None),
            failure: UnsafeCell::new(None),
        }
    }
}

struct Route {
    registration: Arc<Registration>,
    floor: u64,
}

struct Shard {
    stream: FsEventStreamRef,
    routes: BTreeMap<PathBuf, Vec<Route>>,
    pending: Vec<Route>,
    load: usize,
    armed: usize,
    last_id: u64,
    replaying: bool,
    purge: bool,
    #[cfg(test)]
    arms: u64,
}

impl Shard {
    const fn new() -> Self {
        Self {
            stream: std::ptr::null_mut(),
            routes: BTreeMap::new(),
            pending: Vec::new(),
            load: 0,
            armed: 0,
            last_id: 0,
            replaying: false,
            purge: false,
            #[cfg(test)]
            arms: 0,
        }
    }
}

struct State {
    shards: Vec<Shard>,
}

// SAFETY: the stream handles are only used under the state lock or by arm
// custody on the hub's queue.
unsafe impl Send for State {}

impl State {
    fn least_loaded(&mut self) -> (usize, &mut Shard) {
        let full = self.shards.len() == SHARDS;
        let best = self
            .shards
            .iter()
            .enumerate()
            .min_by_key(|(_, shard)| shard.load)
            .map(|(index, shard)| (index, shard.load));
        let index = match best {
            Some((index, load)) if load == 0 || full => index,
            _ => self.shards.len(),
        };
        if index == self.shards.len() {
            self.shards.push(Shard::new());
        }
        let shard = self.shards.get_mut(index).unwrap_or_else(|| unreachable!());
        (index, shard)
    }

    fn of(&mut self, registration: &Registration) -> Option<&mut Shard> {
        self.shards
            .get_mut(registration.shard.load(Ordering::Relaxed))
    }
}

pub(super) struct Hub {
    api: &'static Api,
    queue: DispatchQueue,
    // Held by whoever replaces a shard's stream.
    arm: Mutex<()>,
    state: Mutex<State>,
}

// SAFETY: the queue is a process-lifetime serial dispatch queue, safe to use
// from any thread; everything else is behind the locks.
unsafe impl Send for Hub {}
// SAFETY: as for `Send`.
unsafe impl Sync for Hub {}

impl Hub {
    /// The hub, made with its queue on first use outside finite original work.
    pub(super) fn get<W: SourceDemandReadWork>(work: &mut W) -> Result<&'static Self, DemandError> {
        static LOADING: Mutex<()> = Mutex::new(());
        let api = Api::get(work)?;
        if let Some(hub) = HUB.get() {
            return Ok(hub);
        }
        // The shared queue is process custody, like the framework table.
        if work.is_native_original() {
            return Err(DemandError::NativeWorkUnavailable);
        }
        let _loading = LOADING.lock().map_err(|_| DemandError::WorkerUnavailable)?;
        if let Some(hub) = HUB.get() {
            return Ok(hub);
        }
        native_allocate(work, 0)?;
        // SAFETY: the label is terminated and null attributes select a serial queue.
        let queue =
            unsafe { dispatch_queue_create(c"acyclic.fsevents".as_ptr(), std::ptr::null()) };
        if queue.is_null() {
            return Err(native_error("cannot create the FSEvents dispatch queue"));
        }
        Ok(HUB.get_or_init(|| Self {
            api,
            queue,
            arm: Mutex::new(()),
            state: Mutex::new(State { shards: Vec::new() }),
        }))
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn current_event_id(&self) -> u64 {
        // SAFETY: reads the most recent system-wide event id.
        unsafe { (self.api.current_event_id)() }
    }

    fn context(&self) -> *mut c_void {
        std::ptr::from_ref(self).cast_mut().cast()
    }

    /// Routes events beneath the registration's roots to its handler from
    /// now on. Arming never waits for the queue, so a handler that blocks
    /// delays no registration, and one that registers is armed at once.
    pub(super) fn register(&self, registration: &Arc<Registration>) -> Result<(), DemandError> {
        {
            let mut state = self.state();
            let (index, shard) = state.least_loaded();
            shard.load += registration.roots.len();
            registration.shard.store(index, Ordering::Relaxed);
            // It starts when its stream does, as a stream of its own would.
            shard.pending.push(Route {
                registration: Arc::clone(registration),
                floor: AT_ARM,
            });
        }
        self.arm_all();
        if registration.failed.load(Ordering::Acquire) {
            return Err(native_error("cannot start an FSEvents stream"));
        }
        if !registration.routed.load(Ordering::Acquire) {
            // Its shard is replaying and arms it once that is done; from
            // now, its replay covers every event the registration may want.
            let now = self.current_event_id();
            let mut state = self.state();
            let pending = state.of(registration).map(|shard| &mut shard.pending);
            for route in pending.into_iter().flatten() {
                if Arc::ptr_eq(&route.registration, registration) && route.floor == AT_ARM {
                    route.floor = now;
                }
            }
        }
        Ok(())
    }

    /// Stops routing to the registration. Its handler may still be running
    /// a batch until the queue drains.
    pub(super) fn unregister(&self, registration: &Arc<Registration>) {
        let mut state = self.state();
        if !registration.live.swap(false, Ordering::AcqRel) {
            return;
        }
        let Some(shard) = state.of(registration) else {
            return;
        };
        shard.load -= registration.roots.len();
        let mine = |route: &Route| Arc::ptr_eq(&route.registration, registration);
        if let Some(index) = shard.pending.iter().position(mine) {
            shard.pending.swap_remove(index);
            return;
        }
        for root in &registration.roots {
            if let Some(routes) = shard.routes.get_mut(root) {
                routes.retain(|route| !mine(route));
                if routes.is_empty() {
                    shard.routes.remove(root);
                }
            }
        }
        let live = shard.routes.len();
        if shard.armed.saturating_sub(live) > live && !shard.purge {
            shard.purge = true;
            drop(state);
            // SAFETY: the hub is static; the job runs on its serial queue.
            unsafe { dispatch_async_f(self.queue, self.context(), arm_later) };
        }
    }

    /// Waits until no handler runs a batch taken before now.
    pub(super) fn drain(&self) {
        // SAFETY: the queue lives for the process; the barrier has no context.
        unsafe { dispatch_sync_f(self.queue, std::ptr::null_mut(), drain) };
    }

    pub(super) fn queue(&self) -> DispatchQueue {
        self.queue
    }

    /// Re-arms every shard that has grown or holds mostly dead roots. Arm
    /// custody serializes it; its holder never waits for the queue.
    fn arm_all(&self) {
        // Failure jobs are enqueued under routing custody, but cannot enter a
        // handler until this original arming owner has released its lock.
        let suspended = Suspended::new(self.queue);
        {
            let _arm = self.arm.lock().unwrap_or_else(PoisonError::into_inner);
            let shards = self.state().shards.len();
            for index in 0..shards {
                self.arm_shard(index);
            }
        }
        drop(suspended);
    }

    fn arm_shard(&self, index: usize) {
        let now = self.current_event_id();
        let (mut batch, roots, since, old) = {
            let mut state = self.state();
            let Some(shard) = state.shards.get_mut(index) else {
                return;
            };
            // History arrives in path order, so a replaying stream has no
            // id below which it has delivered everything; it re-arms when done.
            if (shard.pending.is_empty() && !shard.purge) || shard.replaying {
                return;
            }
            let mut batch = std::mem::take(&mut shard.pending);
            for route in &mut batch {
                if route.floor == AT_ARM {
                    route.floor = now;
                }
            }
            let mut roots = shard.routes.keys().cloned().collect::<Vec<_>>();
            for route in &batch {
                if route.registration.live.load(Ordering::Acquire) {
                    roots.extend(route.registration.roots.iter().cloned());
                }
            }
            roots.sort_unstable();
            roots.dedup();
            let since = batch
                .iter()
                .map(|route| route.floor)
                .chain((!shard.stream.is_null()).then_some(shard.last_id))
                .min()
                .unwrap_or(EVENT_ID_SINCE_NOW);
            (batch, roots, since, shard.stream)
        };
        // No callback runs between the new stream's start and the switch to
        // it, so none of its events finds it not yet the shard's.
        let suspended = Suspended::new(self.queue);
        let stream = if roots.is_empty() {
            Ok(std::ptr::null_mut())
        } else {
            self.create(&roots, since)
        };
        let mut state = self.state();
        // Shards are never removed.
        let Some(shard) = state.shards.get_mut(index) else {
            return;
        };
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                batch.retain(|route| {
                    if route.registration.live.swap(false, Ordering::AcqRel) {
                        route.registration.failed.store(true, Ordering::Release);
                        shard.load -= route.registration.roots.len();
                        true
                    } else {
                        false
                    }
                });
                let error = Arc::new(error);
                for route in batch {
                    // Publish on the SAME serial queue before releasing routing
                    // custody. Unregister's barrier therefore includes failures,
                    // even when another registering thread owns this failed arm.
                    // SAFETY: this failed registration is removed from routing;
                    // its sole writer runs before the queue can consume the slot.
                    unsafe {
                        *route.registration.failure.get() = Some(Arc::clone(&error));
                        dispatch_async_f(
                            self.queue,
                            Arc::into_raw(route.registration).cast_mut().cast(),
                            failed_registration,
                        );
                    }
                }
                return;
            }
        };
        // A callback already running when the queue was suspended may have
        // delivered more since `since` was read. Now none can: the old stream
        // delivered every event of its roots up to `last`, and the new one
        // replays from no later than that.
        let last = shard.last_id;
        for routes in shard.routes.values_mut() {
            for route in routes {
                route.floor = route.floor.max(last);
            }
        }
        for route in batch {
            if !route.registration.live.load(Ordering::Acquire) {
                continue;
            }
            for root in &route.registration.roots {
                shard.routes.entry(root.clone()).or_default().push(Route {
                    registration: Arc::clone(&route.registration),
                    floor: route.floor,
                });
            }
            route.registration.routed.store(true, Ordering::Release);
        }
        shard.stream = stream;
        shard.last_id = since;
        shard.replaying = !stream.is_null();
        shard.armed = roots.len();
        shard.purge = false;
        #[cfg(test)]
        {
            shard.arms += 1;
        }
        drop(state);
        if !old.is_null() {
            // SAFETY: arm custody owns the replaced stream. Any of its
            // callbacks already queued finds it no shard's and ignores it.
            unsafe {
                (self.api.stream_stop)(old);
                (self.api.stream_invalidate)(old);
                dispatch_async_f(self.queue, old, release_stream);
            }
        }
        drop(suspended);
    }

    fn create(&self, roots: &[PathBuf], since: u64) -> Result<FsEventStreamRef, DemandError> {
        // The shared streams are process custody: no one watch's work pays
        // for the other roots its re-arm restates.
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let (paths, bytes) = self.api.path_array(roots, &mut work)?;
        let context = FsEventStreamContext {
            version: 0,
            info: self.context(),
            retain: None,
            release: None,
            copy_description: None,
        };
        // SAFETY: the context points at the static hub; a successful stream
        // retains the path array, which this attempt then releases.
        let stream = unsafe {
            (self.api.stream_create)(
                std::ptr::null(),
                hub_callback,
                &raw const context,
                paths,
                since,
                0.0,
                CREATE_FILE_EVENTS | CREATE_NO_DEFER,
            )
        };
        // SAFETY: path_array returned the owned CF reference released exactly once here.
        unsafe { (self.api.release)(paths) };
        let _ = work.release(bytes);
        if stream.is_null() {
            return Err(native_error("cannot create an FSEvents stream"));
        }
        // SAFETY: both handles are live; the stream retains the queue.
        unsafe { (self.api.stream_set_dispatch_queue)(stream, self.queue) };
        // SAFETY: the stream has its context and serial queue installed.
        if unsafe { (self.api.stream_start)(stream) } == 0 {
            // SAFETY: the scheduled, unstarted stream is ours alone.
            unsafe {
                (self.api.stream_invalidate)(stream);
                (self.api.stream_release)(stream);
            }
            return Err(native_error("cannot start an FSEvents stream"));
        }
        Ok(stream)
    }

    /// Routes one native batch, then runs each handler on its share outside
    /// the lock, so a handler may register or stop watches.
    fn deliver(
        &self,
        stream: FsEventStreamRef,
        paths: &[*const c_char],
        flags: &[u32],
        ids: &[u64],
    ) {
        let mut batches = Vec::<Batch>::new();
        let mut slots = HashMap::<*const Registration, usize>::new();
        let mut rearm = false;
        {
            let mut state = self.state();
            let Some(shard) = state.shards.iter_mut().find(|shard| shard.stream == stream) else {
                return;
            };
            for (event, ((&path, &flag), &id)) in paths.iter().zip(flags).zip(ids).enumerate() {
                if flag & HISTORY_DONE != 0 {
                    // The replay held every event of the shard's roots up to
                    // the last it delivered; live events can restate some.
                    let last = shard.last_id;
                    for route in shard.routes.values_mut().flatten() {
                        route.floor = route.floor.max(last);
                    }
                    shard.replaying = false;
                    rearm = !shard.pending.is_empty() || shard.purge;
                    continue;
                }
                shard.last_id = shard.last_id.max(id);
                let mut route = |route: &Route| {
                    if id <= route.floor || !route.registration.live.load(Ordering::Acquire) {
                        return;
                    }
                    let slot = *slots
                        .entry(Arc::as_ptr(&route.registration))
                        .or_insert_with(|| {
                            batches.push(Batch::new(&route.registration));
                            batches.len() - 1
                        });
                    if let Some(batch) = batches.get_mut(slot) {
                        let added = batch.push(event, path, flag);
                        #[cfg(test)]
                        if added {
                            batch.ids.push(id);
                        }
                        #[cfg(not(test))]
                        let _ = added;
                    }
                };
                if flag & (USER_DROPPED | KERNEL_DROPPED) != 0 {
                    shard.routes.values().flatten().for_each(&mut route);
                    continue;
                }
                // SAFETY: the callback passes a live NUL-terminated path.
                let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
                let target = Path::new(OsStr::from_bytes(bytes));
                for ancestor in target.ancestors() {
                    if let Some(routes) = shard.routes.get(ancestor) {
                        routes.iter().for_each(&mut route);
                    }
                }
                // A rescan or mount covers every root beneath its path too.
                if flag & (MUST_SCAN_SUBDIRS | MOUNT | UNMOUNT) != 0 {
                    let beneath = shard
                        .routes
                        .range::<Path, _>((Bound::Excluded(target), Bound::Unbounded))
                        .take_while(|(root, _)| root.starts_with(target));
                    for (_, routes) in beneath {
                        routes.iter().for_each(&mut route);
                    }
                }
            }
        }
        for batch in batches {
            if batch.registration.live.load(Ordering::Acquire) {
                // A panicking handler loses its batch, not the hub.
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    (batch.registration.handler)(Ok(EventBatch {
                        paths: &batch.paths,
                        flags: &batch.flags,
                        #[cfg(test)]
                        ids: &batch.ids,
                    }));
                }));
            }
        }
        if rearm {
            self.arm_all();
        }
    }

    #[cfg(test)]
    fn streams(&self) -> usize {
        self.state()
            .shards
            .iter()
            .filter(|shard| !shard.stream.is_null())
            .count()
    }

    /// Re-arms the registration's shard as if it had grown.
    #[cfg(test)]
    fn rearm(&self, registration: &Registration) {
        if let Some(shard) = self.state().of(registration) {
            shard.purge = true;
        }
        self.arm_all();
    }

    #[cfg(test)]
    fn arms(&self, registration: &Registration) -> u64 {
        self.state().of(registration).map_or(0, |shard| shard.arms)
    }
}

/// One registration's share of a native batch.
struct Batch {
    registration: Arc<Registration>,
    last: Option<usize>,
    paths: Vec<*const c_char>,
    flags: Vec<u32>,
    #[cfg(test)]
    ids: Vec<u64>,
}

impl Batch {
    fn new(registration: &Arc<Registration>) -> Self {
        Self {
            registration: Arc::clone(registration),
            last: None,
            paths: Vec::new(),
            flags: Vec::new(),
            #[cfg(test)]
            ids: Vec::new(),
        }
    }

    /// Adds an event once, however many of the registration's roots it is beneath.
    /// Whether the event was new to the batch.
    fn push(&mut self, event: usize, path: *const c_char, flags: u32) -> bool {
        let added = self.last != Some(event);
        if added {
            self.last = Some(event);
            self.paths.push(path);
            self.flags.push(flags);
        }
        added
    }
}

extern "C" fn hub_callback(
    stream: FsEventStreamRef,
    info: *mut c_void,
    event_count: usize,
    event_paths: *mut c_void,
    event_flags: *const u32,
    event_ids: *const u64,
) {
    let _queue = OnQueue::enter();
    // SAFETY: `info` is the static hub, and without
    // `kFSEventStreamCreateFlagUseCFTypes` the paths are an array of
    // `event_count` C strings, with as many flags and ids.
    let (hub, paths, flags, ids) = unsafe {
        (
            &*info.cast::<Hub>(),
            std::slice::from_raw_parts(event_paths.cast::<*const c_char>(), event_count),
            std::slice::from_raw_parts(event_flags, event_count),
            std::slice::from_raw_parts(event_ids, event_count),
        )
    };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        hub.deliver(stream, paths, flags, ids);
    }));
}

extern "C" fn failed_registration(info: *mut c_void) {
    let _queue = OnQueue::enter();
    // SAFETY: the failed arm gave this queue exactly one raw Arc reference.
    let registration = unsafe { Arc::from_raw(info.cast::<Registration>()) };
    // SAFETY: submission precedes this sole serial-queue consumer.
    let error = unsafe { &mut *registration.failure.get() }.take();
    if let Some(error) = error {
        let _ = catch_unwind(AssertUnwindSafe(|| (registration.handler)(Err(error))));
    }
}

extern "C" fn arm_later(hub: *mut c_void) {
    let _queue = OnQueue::enter();
    // SAFETY: jobs carry the static hub.
    let hub = unsafe { &*hub.cast::<Hub>() };
    let _ = catch_unwind(AssertUnwindSafe(|| hub.arm_all()));
}

/// Holds the hub's queue suspended; a block already running finishes.
struct Suspended(DispatchQueue);

impl Suspended {
    fn new(queue: DispatchQueue) -> Self {
        // SAFETY: the queue lives for the process; the guard resumes it once.
        unsafe { dispatch_suspend(queue) };
        Self(queue)
    }
}

impl Drop for Suspended {
    fn drop(&mut self) {
        // SAFETY: balances the suspension this guard made.
        unsafe { dispatch_resume(self.0) };
    }
}

extern "C" fn release_stream(stream: *mut c_void) {
    if let Some(hub) = HUB.get() {
        // SAFETY: the stream was stopped and invalidated, and every callback
        // queued before this has run.
        unsafe { (hub.api.stream_release)(stream) };
    }
}
