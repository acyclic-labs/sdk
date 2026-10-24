use crate::sync::{Mutex as QueueMutex, mpsc};
use crate::*;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use admission::poll_submission_with;
pub(crate) use admission::{
    AdmissionState, CapacityWaiter, capacity_waiter, notify_capacity_released, poll_submission,
    prepay_capacity_waiter, remove_capacity_waiter, wake_capacity_waiter,
};
pub(crate) use job::NativeJob;

mod admission;
mod job;

pub(crate) struct NativeWorkers {
    pub(crate) sender: mpsc::SyncSender<NativeJob>,
    pub(crate) admission: Arc<QueueMutex<AdmissionState>>,
    /// Worker threads draining `sender`. Private: a test reads it only through
    /// [`NativeWorkers::exclusive_threads`], which proves the test owns the pool.
    #[cfg(test)]
    threads: usize,
    #[cfg(feature = "test-support")]
    pub(crate) host_pool: Arc<crate::testing::HostPool>,
}

#[cfg(test)]
impl NativeWorkers {
    /// A one-worker pool around `sender`, for tests that drive its queue by hand.
    pub(crate) fn single(sender: mpsc::SyncSender<NativeJob>, admission: AdmissionState) -> Self {
        Self {
            sender,
            admission: Arc::new(QueueMutex::new(admission)),
            threads: 1,
            #[cfg(feature = "test-support")]
            host_pool: crate::testing::HostPool::new(1),
        }
    }

    /// The worker count, for a test that occupies every worker of this pool.
    ///
    /// The global pools are fixed-size and shared by every test in the
    /// process. Two such fixtures each hold part of the pool while waiting for
    /// the rest (a circular wait), and while one holds it every other test's
    /// host job queues behind it; a seeded driver whose job meets the full
    /// pool also yields at physical startup, adding a poll to its replayed
    /// trace. So this panics unless the process runs only
    /// the calling test (see [`crate::tests::passed_in_exclusive_process`]).
    pub(crate) fn exclusive_threads(&self) -> usize {
        assert!(
            crate::tests::in_exclusive_process(),
            "a test that occupies every worker of a process-global pool must run alone \
             (crate::tests::passed_in_exclusive_process)"
        );
        self.threads
    }
}

// Receive lock -> channel lock. Both are released before queue admission, and
// every completion/callback runs after all three locks are released.
pub(crate) fn receive_job(
    receiver: &QueueMutex<mpsc::Receiver<NativeJob>>,
    admission: &QueueMutex<AdmissionState>,
) -> Option<NativeJob> {
    let job = receiver
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .recv()
        .ok()?;
    notify_capacity_released(admission);
    Some(job)
}

pub(crate) static NATIVE_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-native-io", |parallelism| parallelism.clamp(2, 4)));

pub(crate) static BLOCKING_WORKERS: LazyLock<io::Result<NativeWorkers>> = LazyLock::new(|| {
    start_workers("acyclic-host-io", |parallelism| {
        parallelism.saturating_mul(2).clamp(2, 16)
    })
});

pub(crate) static IO_SERVICES_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initializes the existing native and host I/O services before measured requests are admitted.
pub fn initialize_native_io_services() -> io::Result<()> {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::new()));
    workers(&NATIVE_WORKERS, false)?;
    workers(&BLOCKING_WORKERS, false)?;
    IO_SERVICES_INITIALIZED.store(true, Ordering::Release);
    Ok(())
}

pub(crate) fn workers(
    slot: &'static LazyLock<io::Result<NativeWorkers>>,
    measured: bool,
) -> io::Result<&'static NativeWorkers> {
    if measured && !IO_SERVICES_INITIALIZED.load(Ordering::Acquire) {
        return Err(io::ErrorKind::NotConnected.into());
    }
    slot.as_ref()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

pub(crate) fn start_workers(
    name: &str,
    worker_count: impl FnOnce(usize) -> usize,
) -> io::Result<NativeWorkers> {
    let worker_count =
        worker_count(std::thread::available_parallelism().map_or(1, std::num::NonZero::get));
    let (sender, receiver) = mpsc::sync_channel::<NativeJob>(worker_count * 4);
    let receiver = Arc::new(QueueMutex::new(receiver));
    let admission = Arc::new(QueueMutex::new(AdmissionState::new()));
    #[cfg(feature = "test-support")]
    let host_pool = crate::testing::HostPool::new(worker_count);
    for worker_index in 0..worker_count {
        let receiver = Arc::clone(&receiver);
        let admission = Arc::clone(&admission);
        #[cfg(feature = "test-support")]
        let host_pool = host_pool.clone();
        std::thread::Builder::new()
            .name(format!("{name}-{worker_index}"))
            .spawn(move || {
                while let Some(job) = receive_job(&receiver, &admission) {
                    #[cfg(feature = "test-support")]
                    let occupant = job.may_wait_for_driver().then(|| host_pool.unscoped());
                    let waker = job.run();
                    #[cfg(feature = "test-support")]
                    drop(occupant);
                    if let Some(waker) = waker {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            waker.wake();
                        }));
                    }
                }
            })?;
    }
    Ok(NativeWorkers {
        sender,
        admission,
        #[cfg(test)]
        threads: worker_count,
        #[cfg(feature = "test-support")]
        host_pool,
    })
}
