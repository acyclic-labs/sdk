use crate::native_mount::{MountSourceError, NativeMountCallbackGuard, NativeMountError, NativeMountStartupAdmissionError};
use crate::heap_future::in_heap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Clone)]
pub(in crate::native_mount) struct CallbackRuntime {
    handle: tokio::runtime::Handle,
}

static CALLBACK_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, Arc<std::io::Error>>> = OnceLock::new();

pub(in crate::native_mount) struct CallbackWait {
    thread: std::thread::Thread,
    woken: AtomicBool,
    bytes: u64,
    _allocation: Option<crate::NativeAllocation>,
}

impl CallbackWait {
    fn new(bytes: u64, allocation: Option<crate::NativeAllocation>) -> Arc<Self> {
        Arc::new(Self {
            thread: std::thread::current(), woken: AtomicBool::new(false), bytes,
            _allocation: allocation,
        })
    }

    pub(in crate::native_mount) fn logical_owned_bytes(&self) -> u64 { self.bytes }

    fn poll<F: Future + Unpin>(self: Arc<Self>, mut future: F) -> F::Output {
        let waker = Waker::from(Arc::clone(&self));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut context) {
                drop(future);
                return output;
            }
            while !self.woken.swap(false, Ordering::Acquire) { std::thread::park(); }
        }
    }
}

impl Wake for CallbackWait {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

struct Allocation<'a> {
    meter: &'a dyn acyclic_native_runtime::NativeReadMeter,
    bytes: u64,
}

impl Drop for Allocation<'_> {
    fn drop(&mut self) { self.meter.release(self.bytes); }
}

/// Initializes the process-shared callback runtime outside a bounded invocation.
pub fn initialize_native_mount_services() -> Result<(), NativeMountError> {
    CallbackRuntime::create().map(|_| ())
}

impl CallbackRuntime {
    pub(in crate::native_mount) fn create() -> Result<Self, NativeMountError> {
        CALLBACK_RUNTIME.get_or_init(|| tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2).enable_all().thread_name("acyclic-fs-mount").build().map_err(Arc::new));
        Self::existing()
    }

    pub(in crate::native_mount) fn existing() -> Result<Self, NativeMountError> {
        Self::existing_startup().map_err(Into::into)
    }

    pub(in crate::native_mount) fn existing_startup() -> Result<Self, NativeMountStartupAdmissionError> {
        match CALLBACK_RUNTIME.get().ok_or(NativeMountStartupAdmissionError::ServicesCold)? {
            Ok(runtime) => Ok(Self { handle: runtime.handle().clone() }),
            Err(error) => Err(NativeMountStartupAdmissionError::ServiceUnavailable(Arc::clone(error))),
        }
    }

    fn enter<T>(&self, poll: impl FnOnce() -> T) -> T {
        let poll = || {
            let _runtime = self.handle.enter();
            let _inline = acyclic_native_runtime::InlineBlocking::enter();
            poll()
        };
        match tokio::runtime::Handle::try_current() {
            Ok(current) if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
                tokio::task::block_in_place(poll),
            _ => poll(),
        }
    }

    pub(in crate::native_mount) fn prepare_wait(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<Arc<CallbackWait>, MountSourceError> {
        work.check().map_err(crate::native_mount::lazy::mount_demand_error)?;
        let bytes = std::mem::size_of::<CallbackWait>() as u64;
        work.claim_allocation(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED).map_err(crate::native_mount::lazy::mount_demand_error)?;
        Ok(CallbackWait::new(bytes, None))
    }

    pub(in crate::native_mount) fn wait_prepared<F: Future>(
        &self, wait: Arc<CallbackWait>, create: impl FnOnce() -> F,
    ) -> F::Output {
        self.enter(|| {
            let mut future = std::pin::pin!(tokio::task::unconstrained(create()));
            wait.poll(future.as_mut())
        })
    }

    pub(in crate::native_mount) fn verify_native_callback_guard(
        &self, guard: &dyn NativeMountCallbackGuard, work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.verify(work, || guard.check(work))
    }

    pub(in crate::native_mount) fn verify_native_callback_write_guard(
        &self, guard: &dyn NativeMountCallbackGuard, work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.verify(work, || guard.check_write(work))
    }

    fn verify<F: Future<Output = Result<(), NativeMountError>>>(
        &self, work: &crate::NativeMaterializationWork,
        create: impl FnOnce() -> Result<F, NativeMountError>,
    ) -> Result<(), NativeMountError> {
        let allocation = work.claim(std::mem::size_of::<CallbackWait>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        self.wait_prepared(CallbackWait::new(0, Some(allocation)), || async { create()?.await })
    }

    pub(in crate::native_mount) fn block_on_measured<F: Future>(
        &self, create: impl FnOnce() -> F, meter: &dyn acyclic_native_runtime::NativeReadMeter,
    ) -> std::io::Result<F::Output> {
        self.enter(|| {
            let mut allocation = Allocation { meter, bytes: 0 };
            let (future, _) = crate::heap_future::in_heap_admitted(
                (create, &mut allocation),
                |(_, allocation), bytes| {
                    meter.allocate(bytes)?;
                    allocation.bytes = bytes;
                    Ok::<_, std::io::Error>(())
                },
                |(create, _)| tokio::task::unconstrained(create()),
            )?;
            let bytes = std::mem::size_of::<CallbackWait>() as u64;
            meter.allocate(bytes)?;
            let _wake_allocation = Allocation { meter, bytes };
            Ok(CallbackWait::new(0, None).poll(future))
        })
    }

    pub(super) fn block_on<F: Future>(&self, create: impl FnOnce() -> F) -> F::Output {
        self.enter(|| CallbackWait::new(0, None).poll(tokio::task::unconstrained(in_heap(create))))
    }

    pub(in crate::native_mount) fn wait<T, F: Future<Output = Result<T, MountSourceError>>>(
        &self, create: impl FnOnce() -> F,
    ) -> Result<T, MountSourceError> {
        self.block_on(|| tokio::time::timeout(super::CALLBACK_TIMEOUT, create()))
            .map_err(|_| MountSourceError::Stale)?
    }
}
