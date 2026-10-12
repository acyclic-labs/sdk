use super::*;
use crate::native_mount::NativeMountStartupAdmissionError;

/// Blocking bridge into the process-shared callback runtime.
#[derive(Clone)]
pub(in crate::native_mount) struct CallbackRuntime {
    handle: tokio::runtime::Handle,
}

static CALLBACK_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, Arc<std::io::Error>>> =
    OnceLock::new();

struct ParkedCallback {
    woken: StdMutex<bool>,
    changed: std::sync::Condvar,
    _allocation: Option<crate::NativeAllocation>,
}

impl Wake for ParkedCallback {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        *self.woken.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_one();
    }
}

/// Wake owner reserved before the future borrows its invocation.
pub(in crate::native_mount) struct PreparedCallbackWait {
    parked: Arc<ParkedCallback>,
    bytes: u64,
}

impl PreparedCallbackWait {
    pub(in crate::native_mount) fn logical_owned_bytes(&self) -> u64 {
        self.bytes
    }
}

struct RuntimeAllocation<'a> {
    meter: Option<&'a dyn acyclic_native_runtime::NativeReadMeter>,
    bytes: u64,
}

impl RuntimeAllocation<'_> {
    fn reserve(&mut self, bytes: u64) -> std::io::Result<()> {
        let total = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| std::io::Error::other("callback allocation overflow"))?;
        if let Some(meter) = self.meter {
            meter.allocate(bytes)?;
        }
        self.bytes = total;
        Ok(())
    }
}

impl Drop for RuntimeAllocation<'_> {
    fn drop(&mut self) {
        if let Some(meter) = self.meter {
            meter.release(self.bytes);
        }
    }
}

/// Initializes callback services outside a bounded invocation.
pub fn initialize_native_mount_services() -> Result<(), NativeMountError> {
    CallbackRuntime::create().map(|_| ())
}

impl CallbackRuntime {
    pub(in crate::native_mount) fn create() -> Result<Self, NativeMountError> {
        CALLBACK_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("acyclic-fs-mount")
                .build()
                .map_err(Arc::new)
        });
        Self::existing()
    }

    pub(in crate::native_mount) fn existing() -> Result<Self, NativeMountError> {
        Self::existing_startup().map_err(Into::into)
    }

    pub(in crate::native_mount) fn existing_startup()
    -> Result<Self, NativeMountStartupAdmissionError> {
        match CALLBACK_RUNTIME
            .get()
            .ok_or(NativeMountStartupAdmissionError::ServicesCold)?
        {
            Ok(runtime) => Ok(Self {
                handle: runtime.handle().clone(),
            }),
            Err(error) => Err(NativeMountStartupAdmissionError::ServiceUnavailable(
                Arc::clone(error),
            )),
        }
    }

    pub(in crate::native_mount) fn prepare_wait(
        &self,
        work: &mut crate::demand::DemandWork,
    ) -> Result<PreparedCallbackWait, MountSourceError> {
        work.check()
            .map_err(crate::native_mount::lazy::mount_demand_error)?;
        let bytes = std::mem::size_of::<ParkedCallback>() as u64;
        work.claim_allocation(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED)
            .map_err(crate::native_mount::lazy::mount_demand_error)?;
        Ok(PreparedCallbackWait {
            parked: Arc::new(ParkedCallback {
                woken: StdMutex::new(false),
                changed: std::sync::Condvar::new(),
                _allocation: None,
            }),
            bytes,
        })
    }

    fn enter<T>(&self, poll: impl FnOnce() -> T) -> T {
        let poll = || {
            let _runtime = self.handle.enter();
            let _inline = acyclic_native_runtime::InlineBlocking::enter();
            poll()
        };
        match tokio::runtime::Handle::try_current() {
            Ok(current)
                if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
            {
                tokio::task::block_in_place(poll)
            }
            _ => poll(),
        }
    }

    pub(in crate::native_mount) fn wait_prepared<F: Future>(
        &self,
        prepared: PreparedCallbackWait,
        create: impl FnOnce() -> F,
    ) -> F::Output {
        self.enter(|| {
            let mut future = std::pin::pin!(tokio::task::unconstrained(create()));
            Self::poll(future.as_mut(), prepared.parked)
        })
    }

    pub(in crate::native_mount) fn verify_callback<
        F: Future<Output = Result<(), NativeMountError>>,
    >(
        &self,
        work: &crate::NativeMaterializationWork,
        create: impl FnOnce() -> Result<F, NativeMountError>,
    ) -> Result<(), NativeMountError> {
        let allocation = work.claim(
            std::mem::size_of::<ParkedCallback>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let prepared = PreparedCallbackWait {
            parked: Arc::new(ParkedCallback {
                woken: StdMutex::new(false),
                changed: std::sync::Condvar::new(),
                _allocation: Some(allocation),
            }),
            bytes: 0,
        };
        self.wait_prepared(prepared, || async { create()?.await })
    }

    pub(in crate::native_mount) fn block_on<F: Future>(
        &self,
        create: impl FnOnce() -> F,
        meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<F::Output> {
        self.enter(|| {
            let mut allocation = RuntimeAllocation { meter, bytes: 0 };
            let (future, _) = crate::heap_future::in_heap_admitted(
                (create, &mut allocation),
                |(_, allocation), bytes| allocation.reserve(bytes),
                |(create, _)| tokio::task::unconstrained(create()),
            )?;
            allocation.reserve(std::mem::size_of::<ParkedCallback>() as u64)?;
            let parked = Arc::new(ParkedCallback {
                woken: StdMutex::new(false),
                changed: std::sync::Condvar::new(),
                _allocation: None,
            });
            Ok(Self::poll(future, parked))
        })
    }

    fn poll<F: Future + Unpin>(mut future: F, parked: Arc<ParkedCallback>) -> F::Output {
        let waker = Waker::from(Arc::clone(&parked));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut context) {
                drop(future);
                return output;
            }
            let mut woken = parked.woken.lock().unwrap_or_else(PoisonError::into_inner);
            while !*woken {
                woken = parked
                    .changed
                    .wait(woken)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            *woken = false;
        }
    }

    pub(in crate::native_mount) fn wait<T, F: Future<Output = Result<T, MountSourceError>>>(
        &self,
        create: impl FnOnce() -> F,
    ) -> Result<T, MountSourceError> {
        self.block_on(|| tokio::time::timeout(CALLBACK_TIMEOUT, create()), None)
            .map_err(capture_io_error)?
            .map_err(|_| MountSourceError::Stale)?
    }
}

pub(super) fn invoke<T, F: Future<Output = Result<T, MountSourceError>>>(
    runtime: &CallbackRuntime,
    work: &crate::NativeMaterializationWork,
    create: impl FnOnce() -> F,
) -> Result<T, MountSourceError> {
    runtime
        .block_on(
            || tokio::time::timeout(CALLBACK_TIMEOUT, create()),
            Some(work.runtime_meter().as_ref()),
        )
        .map_err(|error| MountSourceError::NativeWork(work.runtime_error(error)))?
        .map_err(|_| MountSourceError::Stale)?
}
