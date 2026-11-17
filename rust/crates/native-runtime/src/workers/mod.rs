use crate::sync::{Mutex as QueueMutex, mpsc};
use crate::*;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use admission::poll_submission_with;
pub(crate) use admission::{
    AdmissionState, CapacityWaiter, capacity_waiter, notify_capacity_released, poll_submission,
    prepay_capacity_waiter, remove_capacity_waiter, wake_capacity_waiter,
};
pub(crate) use job::DeferredPoll;
pub(crate) use job::NativeJob;

mod admission;
mod job;

pub(crate) struct NativeWorkers {
    pub(crate) sender: mpsc::SyncSender<NativeJob>,
    pub(crate) admission: Arc<QueueMutex<AdmissionState>>,
    /// Existing service width; batches never create another pool or enlarge it.
    pub(crate) threads: usize,
    service: Option<Service>,
    #[cfg(feature = "test-support")]
    pub(crate) host_pool: Arc<crate::testing::HostPool>,
}

struct Service {
    name: &'static str,
    receiver: Arc<QueueMutex<mpsc::Receiver<NativeJob>>>,
}

/// A dequeued job retains its running slot through execution and unwinding.
pub(crate) struct Running(Arc<QueueMutex<AdmissionState>>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .running_jobs -= 1;
    }
}

impl NativeWorkers {
    fn start(&self, admission: &mut AdmissionState) -> io::Result<()> {
        let Some(service) = &self.service else {
            return Ok(());
        };
        let demand = admission.running_jobs + admission.queued_jobs + 1;
        while admission.started_workers < demand.min(self.threads) {
            let receiver = Arc::clone(&service.receiver);
            let shared_admission = Arc::clone(&self.admission);
            #[cfg(feature = "test-support")]
            let host_pool = self.host_pool.clone();
            std::thread::Builder::new()
                .name(format!("{}-{}", service.name, admission.started_workers))
                .spawn(move || {
                    while let Some((job, _running)) = receive_job(&receiver, &shared_admission) {
                        #[cfg(feature = "test-support")]
                        let occupant = job.may_wait_for_driver().then(|| host_pool.unscoped());
                        let waker = job.run();
                        #[cfg(feature = "test-support")]
                        drop(occupant);
                        wake_capacity_waiter(waker);
                    }
                })?;
            admission.started_workers += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
impl NativeWorkers {
    /// A one-worker pool around `sender`, for tests that drive its queue by hand.
    pub(crate) fn single(sender: mpsc::SyncSender<NativeJob>, admission: AdmissionState) -> Self {
        Self {
            sender,
            admission: Arc::new(QueueMutex::new(admission)),
            threads: 1,
            service: None,
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
    admission: &Arc<QueueMutex<AdmissionState>>,
) -> Option<(NativeJob, Running)> {
    let job = receiver
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .recv()
        .ok()?;
    Some((job, notify_capacity_released(admission)))
}

pub(crate) static NATIVE_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-native-io", parallelism()));

pub(crate) static BLOCKING_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-host-io", parallelism()));

// Persistence never waits for a capture callback or interactive input. Keeping
// it separate prevents a blocked host callback from occupying its producer.
pub(crate) static PERSISTENCE_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-persistence", parallelism()));

// A synchronous inherited sink can retain its write until the peer drains.
// It must not occupy persistence or host-callback capacity. This work class
// shares their admission and terminal implementation, and is created only on
// the first synchronous output submission, never per descriptor or agent.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) static OUTPUT_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-output", parallelism()));

// Host I/O includes file-lock transactions that wait on other owners. Process
// launch, capture turns and their owner callbacks never wait on such a lock,
// so an unrelated command never queues behind them. Created on first use,
// never per process or agent.
pub(crate) static PROCESS_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-process-io", parallelism()));

pub(crate) static IO_SERVICES_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initializes the existing native and host I/O services before measured requests are admitted.
pub fn initialize_native_io_services() -> io::Result<()> {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::new()));
    for slot in [&NATIVE_WORKERS, &BLOCKING_WORKERS] {
        let workers = workers(slot, false)?;
        let mut admission = workers
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        workers.start(&mut admission)?;
    }
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

/// One service worker per available CPU; queue admission supplies backpressure.
pub(crate) fn parallelism() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

fn start_workers(name: &'static str, worker_count: usize) -> io::Result<NativeWorkers> {
    // Every process that does native I/O, product or test, starts here.
    crate::raise_descriptor_limit();
    let (sender, receiver) = mpsc::sync_channel::<NativeJob>(worker_count * 4);
    let admission = Arc::new(QueueMutex::new(AdmissionState::new()));
    #[cfg(feature = "test-support")]
    let host_pool = crate::testing::HostPool::new(worker_count);
    Ok(NativeWorkers {
        sender,
        admission,
        threads: worker_count,
        service: Some(Service {
            name,
            receiver: Arc::new(QueueMutex::new(receiver)),
        }),
        #[cfg(feature = "test-support")]
        host_pool,
    })
}

#[cfg(all(test, not(loom)))]
mod demand_tests;
