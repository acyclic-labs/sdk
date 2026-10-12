use crate::*;
#[cfg(test)]
pub(crate) use admission::poll_submission_with;
pub(crate) use admission::{
    AdmissionState, CapacityWaiter, allocate_capacity_waiter, notify_capacity_released,
    poll_submission, remove_capacity_waiter, wake_capacity_waiter,
};
pub(crate) use job::{NativeJob, NativeUnitOperation};

mod admission;
mod job;

pub(crate) struct NativeWorkers {
    pub(crate) sender: mpsc::SyncSender<NativeJob>,
    pub(crate) admission: Arc<Mutex<AdmissionState>>,
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
    let receiver = Arc::new(Mutex::new(receiver));
    let admission = Arc::new(Mutex::new(AdmissionState::new()));
    for worker_index in 0..worker_count {
        let receiver = Arc::clone(&receiver);
        let admission = Arc::clone(&admission);
        std::thread::Builder::new()
            .name(format!("{name}-{worker_index}"))
            .spawn(move || {
                loop {
                    let job = {
                        let receiver = receiver
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        receiver.recv()
                    };
                    let Ok(job) = job else {
                        break;
                    };
                    notify_capacity_released(&admission);
                    let waker = job.run();
                    if let Some(waker) = waker {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            waker.wake();
                        }));
                    }
                }
            })?;
    }
    Ok(NativeWorkers { sender, admission })
}
