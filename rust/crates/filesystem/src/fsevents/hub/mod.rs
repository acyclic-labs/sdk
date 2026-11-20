//! The process's shared `FSEvents` stream, which every [`EventStream`]
//! registers its roots on.
//!
//! A process can run only about 512 streams, so one stream per watch cannot
//! reach the number of watches the harness keeps. The hub runs one stream
//! on one serial queue. Each event reaches the
//! registrations whose root is its path or a component-boundary ancestor.
//!
//! Native coverage is `/`, independent of the registered paths. A native
//! `RootChanged` has id zero and is not replayed: watching a moved leaf loses
//! its ancestor's rename during a deferred arm. Stable coverage instead replays
//! ordinary directory rename/removal events, which invalidate the routes below
//! them. One native root also keeps descriptor use independent of watch count.
//!
//! A fresh stream starts live before its first registration returns. A
//! registration can also join its covering stream during replay. If pending
//! routes need a replacement stream, it replays history from the older of
//! the last event the shard delivered and each new
//! registration's start id. Every route drops an event at or below its
//! floor, the last id its registration has had or did not want, so a re-arm
//! neither loses nor repeats an event. History arrives in path order, so a
//! shard re-arms only once its replay is done. Its old native stream retires
//! behind the queue's earlier callbacks before the replacement takes that slot.
//! Registrations keep their replay cut while pending, so a blocked handler
//! delays no registration and a handler may register or stop watches. Unregistering only removes routes;
//! the stream stops when its last registration leaves.
//!
//! The current event id is no registration cut: `fseventsd` numbers an event
//! only when it drains the kernel's queue, so a write that returned before the
//! registration can still receive a later id. Registration therefore ends by
//! creating and removing its own marker in the hub's private directory. The
//! kernel queues events in order, so the marker's id is above every earlier
//! write and below every later one. A route delivers nothing until its marker
//! arrives, and the marker's id becomes its floor. History arrives in path
//! order, so a marker seen during replay returns its route to pending, to be
//! replayed from that floor. A stream gap ends the wait: the route must rescan.

use super::*;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{MutexGuard, PoisonError};

#[cfg(feature = "test-support")]
pub mod stress_fixture;
#[cfg(test)]
mod tests;

/// One native producer covers every logical registration.
pub(super) const SHARDS: usize = 1;

/// A registration marker's create and remove.
pub(super) const MARKER_WRITES: WorkCounters = WorkCounters {
    backend_write_operations: 2,
    ..WorkCounters::UNCHARGED
};

static HUB: OnceLock<Hub> = OnceLock::new();

/// One watch's roots and handler, shared by its routes and its owner.
pub(super) struct Registration {
    roots: Vec<PathBuf>,
    handler: Arc<BorrowedEventHandlerFn>,
    live: AtomicBool,
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
    /// The registration's marker while it has not arrived.
    marker: Option<u64>,
}

struct Shard {
    stream: FsEventStreamRef,
    retiring: FsEventStreamRef,
    root: Option<PathBuf>,
    routes: BTreeMap<PathBuf, Vec<Route>>,
    pending: Vec<Route>,
    load: usize,
    last_id: u64,
    replaying: bool,
    purge: bool,
    #[cfg(any(test, feature = "test-support"))]
    arms: u64,
}

impl Shard {
    const fn new() -> Self {
        Self {
            stream: std::ptr::null_mut(),
            retiring: std::ptr::null_mut(),
            root: None,
            routes: BTreeMap::new(),
            pending: Vec::new(),
            load: 0,
            last_id: 0,
            replaying: false,
            purge: false,
            #[cfg(any(test, feature = "test-support"))]
            arms: 0,
        }
    }

    fn route(&mut self, batch: impl IntoIterator<Item = Route>) {
        for route in batch {
            if !route.registration.live.load(Ordering::Acquire) {
                continue;
            }
            for root in &route.registration.roots {
                self.routes.entry(root.clone()).or_default().push(Route {
                    registration: Arc::clone(&route.registration),
                    floor: route.floor,
                    marker: route.marker,
                });
            }
        }
    }

    /// Installs the exact cut of the registration whose marker has `id`.
    fn cut(&mut self, marker: u64, id: u64) {
        for route in &mut self.pending {
            if route.marker == Some(marker) {
                route.marker = None;
                route.floor = id;
            }
        }
        if !self.replaying {
            // Live delivery is in id order: every later event is above `id`.
            for route in self.routes.values_mut().flatten() {
                if route.marker == Some(marker) {
                    route.marker = None;
                    route.floor = id;
                }
            }
            return;
        }
        // History arrives in path order, so later events of these roots may
        // already have passed. The route replays them from its cut.
        let mut cut = None;
        for routes in self.routes.values_mut() {
            routes.retain(|route| {
                if route.marker != Some(marker) {
                    return true;
                }
                cut.get_or_insert_with(|| Arc::clone(&route.registration));
                false
            });
        }
        self.routes.retain(|_, routes| !routes.is_empty());
        if let Some(registration) = cut {
            self.pending.push(Route {
                registration,
                floor: id,
                marker: None,
            });
        }
    }

    /// A gap carries no cut: every waiting route must rescan instead.
    fn end_waits(&mut self) {
        for route in self.routes.values_mut().flatten().chain(&mut self.pending) {
            route.marker = None;
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
    // The canonical private directory of registration markers, and the next one.
    markers: PathBuf,
    next_marker: AtomicU64,
    // Held by whoever replaces a shard's stream.
    arm: Mutex<()>,
    state: Mutex<State>,
    #[cfg(any(test, feature = "test-support"))]
    armed: std::sync::Condvar,
}

// SAFETY: the queue is a process-lifetime serial dispatch queue, safe to use
// from any thread; everything else is behind the locks.
unsafe impl Send for Hub {}
// SAFETY: as for `Send`.
unsafe impl Sync for Hub {}

impl Hub {
    /// Bootstrap process custody once, before finite original work starts.
    pub(super) fn initialize_services() -> notify::Result<()> {
        if HUB.get().is_some() {
            return Ok(());
        }
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Self::get(&mut work).map(|_| ()).map_err(notify_error)
    }

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
        let markers = std::env::temp_dir().join("acyclic-fsevents");
        std::fs::create_dir_all(&markers)?;
        let markers = markers.canonicalize()?;
        // SAFETY: the label is terminated and null attributes select a serial queue.
        let queue =
            unsafe { dispatch_queue_create(c"acyclic.fsevents".as_ptr(), std::ptr::null()) };
        if queue.is_null() {
            return Err(native_error("cannot create the FSEvents dispatch queue"));
        }
        Ok(HUB.get_or_init(|| Self {
            api,
            queue,
            markers,
            next_marker: AtomicU64::new(0),
            arm: Mutex::new(()),
            state: Mutex::new(State { shards: Vec::new() }),
            #[cfg(any(test, feature = "test-support"))]
            armed: std::sync::Condvar::new(),
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

    /// Routes events beneath the registration's roots to its handler, from
    /// exactly after every write that returned before this call. Arming never
    /// waits for the queue, so a handler that blocks delays no registration,
    /// and one that registers is armed at once.
    pub(super) fn register(&self, registration: &Arc<Registration>) -> Result<(), DemandError> {
        let marker = self.install(registration)?;
        if let Err(error) = self.mark(marker) {
            self.unregister(registration);
            return Err(error);
        }
        Ok(())
    }

    /// Covers the registration with a route that waits for its marker.
    fn install(&self, registration: &Arc<Registration>) -> Result<u64, DemandError> {
        let marker = self.next_marker.fetch_add(1, Ordering::Relaxed);
        {
            let mut state = self.state();
            let (index, shard) = state.least_loaded();
            let was_empty = shard.load == 0;
            shard.load += registration.roots.len();
            registration.shard.store(index, Ordering::Relaxed);
            // A replay of this route starts no later than its marker.
            let route = Route {
                registration: Arc::clone(registration),
                floor: self.current_event_id(),
                marker: Some(marker),
            };
            if !shard.stream.is_null()
                && shard.root.as_ref().is_some_and(|root| {
                    registration.roots.iter().all(|path| path.starts_with(root))
                })
            {
                // The state lock serializes delivery and retirement with
                // admission. A retiring stream already cleared this slot.
                if was_empty {
                    shard.purge = false;
                }
                shard.route(std::iter::once(route));
                return Ok(marker);
            }
            shard.pending.push(route);
        }
        self.arm_all();
        if registration.failed.load(Ordering::Acquire) {
            return Err(native_error("cannot start an FSEvents stream"));
        }
        Ok(marker)
    }

    fn marker_path(&self, marker: u64) -> PathBuf {
        self.markers
            .join(format!("{}-{marker}", std::process::id()))
    }

    /// This process's marker at `path`, which is in the marker directory.
    fn marker_of(&self, path: &Path) -> Option<u64> {
        let name = path.file_name()?.to_str()?;
        let (process, marker) = name.split_once('-')?;
        if process.parse::<u32>().ok()? != std::process::id() {
            return None;
        }
        marker.parse().ok()
    }

    /// One event ordered after every write that returned before this call.
    fn mark(&self, marker: u64) -> Result<(), DemandError> {
        let path = self.marker_path(marker);
        drop(std::fs::File::create(&path)?);
        std::fs::remove_file(&path)?;
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
        } else {
            for root in &registration.roots {
                if let Some(routes) = shard.routes.get_mut(root) {
                    routes.retain(|route| !mine(route));
                    if routes.is_empty() {
                        shard.routes.remove(root);
                    }
                }
            }
        }
        if shard.load == 0 && !shard.purge {
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

    /// Re-arms every shard whose coverage grew or whose last route left. Arm
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
        let (mut batch, root, cut, replay) = {
            let mut state = self.state();
            let Some(shard) = state.shards.get_mut(index) else {
                return;
            };
            // History arrives in path order, so a replaying stream has no
            // id below which it has delivered everything; it re-arms when done.
            if (shard.pending.is_empty() && !shard.purge)
                || shard.replaying
                || !shard.retiring.is_null()
            {
                return;
            }
            if !shard.stream.is_null() {
                // WatchRoot retains descriptors along the native root's path.
                // A replacement must take this shard's released native slot,
                // after every old callback, rather than accumulating retired
                // streams behind a suspended queue. Pending floors and last_id
                // retain the replay cut across that event-driven handoff.
                let old = std::mem::replace(&mut shard.stream, std::ptr::null_mut());
                shard.retiring = old;
                drop(state);
                // SAFETY: arm custody owns this stream. Invalidation stops new
                // callbacks; the serial release follows those already queued.
                unsafe {
                    (self.api.stream_stop)(old);
                    (self.api.stream_invalidate)(old);
                    dispatch_async_f(self.queue, old, release_stream);
                }
                return;
            }
            let batch = std::mem::take(&mut shard.pending);
            let covered = !shard.routes.is_empty()
                || batch.iter().any(|route| {
                    route.registration.live.load(Ordering::Acquire)
                        && !route.registration.roots.is_empty()
                });
            let root = covered.then(|| PathBuf::from("/"));
            let cut = batch
                .iter()
                .map(|route| route.floor)
                .chain((!shard.routes.is_empty()).then_some(shard.last_id))
                .min()
                .unwrap_or(0);
            // An existing coverage root marks a handoff. Pending watchers
            // can already have returned during retirement, so their cut is
            // required. At first birth, every registrant still waits for arm
            // custody and can join a live stream before returning.
            (batch, root, cut, shard.root.is_some())
        };
        // No callback runs between the new stream's start and the switch to
        // it, so none of its events finds it not yet the shard's.
        let suspended = Suspended::new(self.queue);
        let since = if replay { cut } else { EVENT_ID_SINCE_NOW };
        let stream = match &root {
            Some(root) => self.create(std::array::from_ref(root), since),
            None => Ok(std::ptr::null_mut()),
        };
        let mut state = self.state();
        // Shards are never removed.
        let Some(shard) = state.shards.get_mut(index) else {
            return;
        };
        if root.is_some() {
            // Routes admitted while this stream was starting can use its
            // coverage without a second arm.
            batch.append(&mut shard.pending);
        }
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                // A replacement now starts after its old native owner retired.
                // If creation fails, every route has lost that producer.
                batch.extend(std::mem::take(&mut shard.routes).into_values().flatten());
                shard.root = None;
                shard.purge = false;
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
                #[cfg(any(test, feature = "test-support"))]
                self.armed.notify_all();
                return;
            }
        };
        // Retirement preserves the previous stream's delivered cut. The new
        // stream replays from no later than that cut, while each original
        // route retains its own already-delivered floor.
        let last = shard.last_id;
        for routes in shard.routes.values_mut() {
            for route in routes {
                route.floor = route.floor.max(last);
            }
        }
        shard.route(batch);
        shard.stream = stream;
        shard.root = root;
        shard.last_id = cut;
        shard.replaying = !stream.is_null() && replay;
        shard.purge = !stream.is_null() && shard.load == 0;
        #[cfg(any(test, feature = "test-support"))]
        {
            shard.arms += 1;
            self.armed.notify_all();
        }
        drop(state);
        drop(suspended);
    }

    fn create(&self, roots: &[PathBuf; 1], since: u64) -> Result<FsEventStreamRef, DemandError> {
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
                CREATE_FILE_EVENTS | CREATE_NO_DEFER | CREATE_WATCH_ROOT,
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
                let stream_gap = flag & (USER_DROPPED | KERNEL_DROPPED) != 0;
                // SAFETY: the callback passes a live NUL-terminated path.
                let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
                let target = Path::new(OsStr::from_bytes(bytes));
                if stream_gap {
                    shard.end_waits();
                } else if target.parent() == Some(self.markers.as_path()) {
                    // Markers are the hub's own events, routed to no one.
                    if let Some(marker) = self.marker_of(target) {
                        shard.cut(marker, id);
                    }
                    continue;
                }
                let moved = flag & ROOT_CHANGED != 0
                    || flag & IS_DIR != 0 && flag & (ITEM_RENAMED | ITEM_REMOVED) != 0;
                let mut route = |route: &Route, routed_flag: u32| {
                    // Native root changes and stream gaps carry no usable cut
                    // for the affected registrations. Synthesized root changes
                    // keep the original event's id and must pass the floor.
                    if ((id <= route.floor || route.marker.is_some())
                        && flag & ROOT_CHANGED == 0
                        && !stream_gap)
                        || !route.registration.live.load(Ordering::Acquire)
                    {
                        return;
                    }
                    let slot = *slots
                        .entry(Arc::as_ptr(&route.registration))
                        .or_insert_with(|| {
                            batches.push(Batch::new(&route.registration));
                            batches.len() - 1
                        });
                    if let Some(batch) = batches.get_mut(slot) {
                        let added = batch.push(event, path, routed_flag);
                        #[cfg(any(test, feature = "test-support"))]
                        if added {
                            batch.ids.push(id);
                        }
                        #[cfg(not(any(test, feature = "test-support")))]
                        let _ = added;
                    }
                };
                if stream_gap {
                    for entry in shard.routes.values().flatten() {
                        route(entry, flag);
                    }
                    continue;
                }
                for ancestor in target.ancestors() {
                    if let Some(routes) = shard.routes.get(ancestor) {
                        for entry in routes {
                            route(
                                entry,
                                flag | if moved && ancestor == target {
                                    ROOT_CHANGED
                                } else {
                                    0
                                },
                            );
                        }
                    }
                }
                // A shared native root can be above a registration's root.
                // Moving either that root or an intervening directory invalidates
                // every route beneath it, just as WatchRoot did per path.
                if moved || flag & (MUST_SCAN_SUBDIRS | MOUNT | UNMOUNT) != 0 {
                    let beneath = shard
                        .routes
                        .range::<Path, _>((Bound::Excluded(target), Bound::Unbounded))
                        .take_while(|(root, _)| root.starts_with(target));
                    for (_, routes) in beneath {
                        for entry in routes {
                            route(entry, flag | if moved { ROOT_CHANGED } else { 0 });
                        }
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
                        #[cfg(any(test, feature = "test-support"))]
                        ids: &batch.ids,
                    }));
                }));
            }
        }
        if rearm {
            self.arm_all();
        }
    }

    #[cfg(feature = "test-support")]
    fn streams(&self) -> usize {
        self.state()
            .shards
            .iter()
            .filter(|shard| !shard.stream.is_null())
            .count()
    }

    /// Re-arms the registration's shard as if it had grown.
    #[cfg(feature = "test-support")]
    fn rearm(&self, registration: &Registration) {
        if let Some(shard) = self.state().of(registration) {
            shard.purge = true;
        }
        self.arm_all();
    }

    #[cfg(feature = "test-support")]
    fn arms(&self, registration: &Registration) -> u64 {
        self.state().of(registration).map_or(0, |shard| shard.arms)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn suspend_after_delivery(&self) -> Suspended {
        extern "C" fn suspend(queue: *mut c_void) {
            // SAFETY: the live serial queue is resumed by the returned guard.
            unsafe { dispatch_suspend(queue) };
        }
        // SAFETY: suspends the queue after all earlier callbacks, before returning to the caller.
        unsafe { dispatch_sync_f(self.queue, self.queue, suspend) };
        Suspended(self.queue)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn wait_for_arm(
        &self,
        registration: &Registration,
        before: u64,
        deadline: std::time::Duration,
    ) -> Result<(), DemandError> {
        let (mut state, _) = self
            .armed
            .wait_timeout_while(self.state(), deadline, |state| {
                !registration.failed.load(Ordering::Acquire)
                    && state
                        .of(registration)
                        .is_some_and(|shard| shard.arms <= before)
            })
            .unwrap_or_else(PoisonError::into_inner);
        if registration.failed.load(Ordering::Acquire) {
            return Err(native_error(
                "FSEvents could not start the replacement stream",
            ));
        }
        if state
            .of(registration)
            .is_some_and(|shard| shard.arms > before)
        {
            return Ok(());
        }
        Err(native_error("FSEvents did not arm a replacement stream"))
    }
}

/// One registration's share of a native batch.
struct Batch {
    registration: Arc<Registration>,
    last: Option<usize>,
    paths: Vec<*const c_char>,
    flags: Vec<u32>,
    #[cfg(any(test, feature = "test-support"))]
    ids: Vec<u64>,
}

impl Batch {
    fn new(registration: &Arc<Registration>) -> Self {
        Self {
            registration: Arc::clone(registration),
            last: None,
            paths: Vec::new(),
            flags: Vec::new(),
            #[cfg(any(test, feature = "test-support"))]
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
        } else if let Some(previous) = self.flags.last_mut() {
            *previous |= flags;
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
        {
            // Keep pointer reuse excluded until the retired shard's marker is
            // cleared. No new stream can occupy its native slot before this.
            let _arm = hub.arm.lock().unwrap_or_else(PoisonError::into_inner);
            // SAFETY: the stream was stopped and invalidated, and every
            // callback queued before this serial release has run.
            unsafe { (hub.api.stream_release)(stream) };
            if let Some(shard) = hub
                .state()
                .shards
                .iter_mut()
                .find(|shard| shard.retiring == stream)
            {
                shard.retiring = std::ptr::null_mut();
            }
        }
        hub.arm_all();
    }
}
