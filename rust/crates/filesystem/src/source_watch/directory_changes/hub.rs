//! One completion port and one thread for every Windows source watch.
//!
//! Each watch's directory handle is associated with the port under the
//! address of the watch's shared state. Every packet on the port, a read's
//! completion or a wake, owns one strong count of that state, taken before
//! the read or wake was issued. The state therefore outlives every packet
//! that names it, and the thread never touches a packet's `OVERLAPPED`.
use super::*;
use std::sync::atomic::AtomicI32;
use std::sync::{OnceLock, Weak};
use windows::Win32::Foundation::{ERROR_NOT_ENOUGH_MEMORY, INVALID_HANDLE_VALUE};
use windows::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY,
    PostQueuedCompletionStatus,
};
use windows::Win32::System::Threading::INFINITE;

std::thread_local! {
    /// Whether this thread is the port's.
    static SERVING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether the caller runs on the port's thread, which collects every
/// completion and so must never wait for one.
pub(super) fn serving() -> bool {
    SERVING.with(std::cell::Cell::get)
}

/// Packets the thread removes from the port per wait.
const BATCH: usize = 64;

struct Port(HANDLE);
// SAFETY: a completion port handle may be used from any thread.
unsafe impl Send for Port {}
// SAFETY: as above; production keeps the handle live for the service's lifetime.
unsafe impl Sync for Port {}

/// One service owns native completion delivery and its retained logical health.
pub(super) struct Hub {
    port: Port,
    failure: AtomicI32,
    watches: Mutex<Registry>,
}

#[derive(Default)]
struct Registry {
    first: usize,
}

/// Links live inside the original funded Shared allocation. Registry locking
/// pins every linked node through its Drop, without retaining the native owner.
pub(super) struct Links {
    previous: usize,
    next: usize,
    registered: bool,
    owner: Weak<Shared>,
}

impl Links {
    pub(super) fn new(owner: Weak<Shared>) -> Self {
        Self {
            previous: 0,
            next: 0,
            registered: false,
            owner,
        }
    }
}

/// The process's service, created with its thread on first use.
static HUB: OnceLock<Result<Arc<Hub>, i32>> = OnceLock::new();

pub(super) fn global() -> Result<Arc<Hub>, DemandError> {
    match HUB.get_or_init(Hub::start) {
        Ok(hub) => {
            hub.check()?;
            Ok(Arc::clone(hub))
        }
        Err(code) => Err(io::Error::from_raw_os_error(*code).into()),
    }
}

impl Hub {
    pub(super) fn start() -> Result<Arc<Self>, i32> {
        let hub = Self::create()?;
        let worker = Arc::clone(&hub);
        let port = hub.port.0;
        std::thread::Builder::new()
            .name("acyclic-source-watch".to_owned())
            .spawn(move || worker.serve())
            .map(|_| hub)
            .map_err(|error| {
                // SAFETY: no thread serves this port, so this is its only owner.
                let _ = unsafe { CloseHandle(port) };
                error
                    .raw_os_error()
                    .unwrap_or(ERROR_NOT_ENOUGH_MEMORY.to_hresult().0)
            })
    }

    fn create() -> Result<Arc<Self>, i32> {
        // SAFETY: creates a port with no file associated yet.
        let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, None, 0, 1) }
            .map_err(|error| error.code().0)?;
        Ok(Arc::new(Self {
            port: Port(port),
            failure: AtomicI32::new(0),
            watches: Mutex::new(Registry::default()),
        }))
    }

    pub(super) fn check(&self) -> Result<(), DemandError> {
        match self.failure.load(Ordering::Acquire) {
            0 => Ok(()),
            code => Err(io::Error::from_raw_os_error(code).into()),
        }
    }

    fn register(&self, shared: &Shared) -> Result<(), DemandError> {
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        self.check()?;
        let mut links = shared
            .hub_links
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if links.registered {
            return Ok(());
        }
        links.next = watches.first;
        links.registered = true;
        if watches.first != 0 {
            // SAFETY: Drop unlinks under this same registry lock, before fields retire.
            let first = unsafe { &*(watches.first as *const Shared) };
            first
                .hub_links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .previous = key(shared);
        }
        watches.first = key(shared);
        Ok(())
    }

    pub(super) fn detach(&self, shared: &Shared) {
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        watches.unlink(shared);
    }

    fn fail(&self, code: i32) {
        {
            let _watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
            if self
                .failure
                .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
        }
        loop {
            let watch = {
                let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
                if watches.first == 0 {
                    break;
                }
                // SAFETY: a linked node remains alive until its Drop takes this lock.
                let shared = unsafe { &*(watches.first as *const Shared) };
                let owner = shared
                    .hub_links
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .owner
                    .upgrade();
                watches.unlink(shared);
                owner
            };
            if let Some(watch) = watch {
                // Logical failure never collects a packet or retires native storage.
                // A sink panic must not strand other watches on the failed service.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    watch.service_failed(code);
                }));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_watch::HostChangeSink;
    use crate::{CancellationToken, WorkBudget};
    use std::os::windows::io::AsHandle;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    struct Lost(AtomicUsize);
    impl HostChangeSink for Lost {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }
        fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
            work.check()
        }
        fn host_changed_wide(
            &self,
            _: &[HostChangeWide<'_>],
            work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            work.check()
        }
        fn delivery_lost(&self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn fatal_port_loss_settles_waiters_once_and_retains_pending_native_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let directory =
            cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let hub = Hub::create().map_err(io::Error::from_raw_os_error)?;
        let lost = Arc::new(Lost(AtomicUsize::new(0)));
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let mut watches = Vec::new();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let mut waiters = Vec::new();
        for _ in 0..2 {
            let delivery = Arc::new(Delivery::new(lost.clone(), &mut work)?);
            let watch = NativeSourceWatch::open_on_hub(
                directory.as_handle(),
                delivery,
                super::super::startup::WatchOptions {
                    pin_name: false,
                    complete: Some(true),
                    recursive: false,
                    maximum_buffer_bytes: MAXIMUM_BUFFER_BYTES,
                },
                || Ok(Arc::clone(&hub)),
                &mut work,
            )?;
            let shared = Arc::clone(&watch.shared);
            let ready = ready_tx.clone();
            let finished = finished_tx.clone();
            waiters.push(std::thread::spawn(move || {
                let reads = shared.lock_reads();
                assert!(reads.pending);
                ready.send(()).expect("test controller");
                // This production wait has no deadline or periodic wake.
                let result = shared.await_completion(&reads);
                finished
                    .send(matches!(result, Err(DemandError::Io(_))))
                    .expect("test controller");
            }));
            watches.push(watch);
        }
        for _ in 0..2 {
            ready_rx.recv_timeout(Duration::from_secs(2))?;
        }
        let retained = watches
            .iter()
            .map(NativeSourceWatch::platform_owned_bytes)
            .collect::<Vec<_>>();
        let worker = Arc::clone(&hub);
        let (stopped_tx, stopped_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            // A real OS failure on an invalid wait source, with the accepted
            // reads still owned by their original live port and packet Arcs.
            worker.serve_port(HANDLE::default());
            stopped_tx.send(()).expect("test controller");
        });
        stopped_rx.recv_timeout(Duration::from_secs(2))?;
        thread.join().expect("completion service");
        for _ in 0..2 {
            assert!(finished_rx.recv_timeout(Duration::from_secs(2))?);
        }
        for waiter in waiters {
            waiter.join().expect("native observer");
        }
        assert_eq!(lost.0.load(Ordering::Acquire), 2);
        let failure = hub.failure.load(Ordering::Acquire);
        assert_ne!(failure, 0);
        hub.fail(failure);
        assert_eq!(lost.0.load(Ordering::Acquire), 2);
        for (watch, retained) in watches.iter().zip(retained) {
            let shared = &watch.shared;
            let references = Arc::strong_count(shared);
            assert!(
                references > 1,
                "the pending packet retains its native owner"
            );
            assert!(hub.wake(shared).is_err());
            assert_eq!(Arc::strong_count(shared), references);
            let before = work.work;
            assert!(
                watch
                    .fence_with_work(Duration::from_secs(1), &mut work)
                    .is_err()
            );
            assert_eq!(work.work, before);
            assert!(!watch.is_stopped());
            assert!(shared.lock_reads().pending);
            assert!(is_open(&shared.directory));
            assert_eq!(watch.platform_owned_bytes(), retained);
        }
        assert_eq!(
            hub.watches
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .first,
            0
        );
        let before = work.work;
        let delivery = Arc::new(Delivery::new(lost, &mut work)?);
        let denied = NativeSourceWatch::open_on_hub(
            directory.as_handle(),
            delivery,
            super::super::startup::WatchOptions {
                pin_name: false,
                complete: Some(true),
                recursive: false,
                maximum_buffer_bytes: MAXIMUM_BUFFER_BYTES,
            },
            || Ok(Arc::clone(&hub)),
            &mut work,
        );
        assert!(matches!(denied, Err(DemandError::Io(_))));
        assert_eq!(work.work, before, "failed service refuses new native work");
        Ok(())
    }
}

impl Registry {
    fn unlink(&mut self, shared: &Shared) {
        let mut links = shared
            .hub_links
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !links.registered {
            return;
        }
        links.registered = false;
        let previous = std::mem::take(&mut links.previous);
        let next = std::mem::take(&mut links.next);
        drop(links);
        if previous == 0 {
            self.first = next;
        } else {
            // SAFETY: all linked node lifetimes and links share the registry lock.
            let previous = unsafe { &*(previous as *const Shared) };
            previous
                .hub_links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .next = next;
        }
        if next != 0 {
            // SAFETY: as above; Drop cannot retire this linked node under our lock.
            let next = unsafe { &*(next as *const Shared) };
            next.hub_links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .previous = previous;
        }
    }
}

fn key(shared: &Shared) -> usize {
    std::ptr::from_ref(shared) as usize
}

/// Routes `directory`'s read completions to the port, keyed by `shared`.
impl Hub {
    pub(super) fn attach(&self, shared: &Shared, directory: HANDLE) -> Result<(), DemandError> {
        self.register(shared)?;
        let port = self.port.0;
        // SAFETY: the port and the open directory handle are both live.
        unsafe { CreateIoCompletionPort(directory, Some(port), key(shared), 0) }
            .map_err(io::Error::from)?;
        self.check()
    }
}

/// Takes the strong count that the packet of the read or wake about to be
/// issued owns.
pub(super) fn retain_for_packet(shared: &Shared) {
    // SAFETY: every `Shared` lives in an `Arc`, which the caller keeps.
    unsafe { Arc::increment_strong_count(std::ptr::from_ref(shared)) };
}

/// Returns the count `retain_for_packet` took, for a read or wake that
/// failed and so queued no packet.
pub(super) fn release_unqueued(shared: &Shared) {
    // SAFETY: the count taken above, which no packet owns; the caller's own
    // `Arc` keeps the state alive.
    unsafe { Arc::decrement_strong_count(std::ptr::from_ref(shared)) };
}

/// Queues a wake that has the thread serve `shared`.
impl Hub {
    pub(super) fn wake(&self, shared: &Shared) -> Result<(), DemandError> {
        self.check()?;
        let port = self.port.0;
        retain_for_packet(shared);
        // SAFETY: a packet without an `OVERLAPPED`; it owns the count just taken.
        if let Err(error) = unsafe { PostQueuedCompletionStatus(port, 0, key(shared), None) } {
            release_unqueued(shared);
            return Err(io::Error::from(error).into());
        }
        Ok(())
    }

    fn serve(&self) {
        self.serve_port(self.port.0);
    }

    fn serve_port(&self, port: HANDLE) {
        SERVING.with(|serving| serving.set(true));
        let mut entries = [OVERLAPPED_ENTRY::default(); BATCH];
        loop {
            let mut removed = 0;
            // SAFETY: the kernel writes at most `entries.len()` entries.
            let waited = unsafe {
                GetQueuedCompletionStatusEx(port, &mut entries, &mut removed, INFINITE, false)
            };
            if let Err(error) = waited {
                self.fail(error.code().0);
                return;
            }
            for entry in entries.iter().take(removed as usize) {
                if entry.lpCompletionKey == 0 {
                    continue;
                }
                // SAFETY: each packet owns one strong count of the state its key
                // names, taken by `retain_for_packet`.
                let shared = unsafe { Arc::from_raw(entry.lpCompletionKey as *const Shared) };
                // Removing the packet completed its read; waiters may go on.
                shared.collected();
                Shared::serve(shared);
            }
        }
    }
}
