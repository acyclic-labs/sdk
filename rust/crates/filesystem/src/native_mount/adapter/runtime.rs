use super::*;
use crate::native_mount::NativeMountStartupAdmissionError;

/// Blocking bridge into the process-shared callback runtime.
#[derive(Clone)]
pub(in crate::native_mount) struct CallbackRuntime {
    handle: tokio::runtime::Handle,
    pub(in crate::native_mount) clock: super::super::clock::Clock,
}

static CALLBACK_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, Arc<std::io::Error>>> =
    OnceLock::new();

#[cfg(test)]
thread_local! {
    pub(super) static BEFORE_PARK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

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
    work: crate::NativeMaterializationWork,
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
///
/// A lazy mount watches its host source, so the mount services include the
/// native watch services: macOS loads the `FSEvents` framework here, where no
/// finite source invocation has to fund it.
pub fn initialize_native_mount_services() -> Result<(), NativeMountError> {
    #[cfg(target_os = "macos")]
    crate::native_mount::darwin_mount::reclaim_stale_mounts()?;
    crate::native_mount::registry::initialize()?;
    crate::watch::initialize_native_watch_services()
        .map_err(|error| NativeMountError::Driver(format!("native watch services: {error}")))?;
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
                clock: super::super::clock::Clock::capture(),
            }),
            Err(error) => Err(NativeMountStartupAdmissionError::ServiceUnavailable(
                Arc::clone(error),
            )),
        }
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

    pub(in crate::native_mount) fn wait_prepared<T, E, F>(
        &self,
        prepared: PreparedCallbackWait,
        create: impl FnOnce() -> F,
    ) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
        E: From<crate::NativeTreeMaterializationError>,
    {
        // Admit the actual future before its constructor can run. Its box dies
        // before the allocation guard, including on cancellation and unwind.
        let mut future =
            self.enter(|| native_future(&prepared.work, || tokio::task::unconstrained(create())))?;
        self.poll_prepared(prepared, Pin::new(&mut future))
    }

    // Callers with existing pinned storage lend only its pointer to the bridge.
    // The lease callback keeps its small future in its own measured stack frame;
    // large callback state uses the admitted box above. Neither is constructed
    // inside the polling frame.
    pub(super) fn poll_prepared<F: Future>(
        &self,
        prepared: PreparedCallbackWait,
        future: Pin<&mut F>,
    ) -> F::Output {
        let PreparedCallbackWait { parked, .. } = prepared;
        self.enter(|| Self::poll(future, &parked))
    }

    pub(in crate::native_mount) fn prepare_native_wait(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<PreparedCallbackWait, crate::NativeTreeMaterializationError> {
        let allocation = work.claim(
            std::mem::size_of::<ParkedCallback>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(PreparedCallbackWait {
            parked: Arc::new(ParkedCallback {
                woken: StdMutex::new(false),
                changed: std::sync::Condvar::new(),
                _allocation: Some(allocation),
            }),
            work: work.clone(),
        })
    }

    // A callback phase lends its original ledger to the future owner. Keeping
    // each phase behind a pointer prevents its state from being embedded in
    // every enclosing callback frame, without losing allocation custody.
    pub(in crate::native_mount) fn prepare_future<F: Future>(
        &self,
        work: &crate::NativeMaterializationWork,
        create: impl FnOnce() -> F,
    ) -> Result<impl Future<Output = F::Output>, crate::NativeTreeMaterializationError> {
        native_future(work, create)
    }

    pub(in crate::native_mount) fn verify_callback<
        F: Future<Output = Result<(), NativeMountError>>,
    >(
        &self,
        work: &crate::NativeMaterializationWork,
        create: impl FnOnce() -> Result<F, NativeMountError>,
    ) -> Result<(), NativeMountError> {
        let prepared = self.prepare_native_wait(work)?;
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
            Ok(Self::poll(future, &parked))
        })
    }

    fn poll<F: Future + Unpin>(mut future: F, parked: &Arc<ParkedCallback>) -> F::Output {
        let waker = Waker::from(Arc::clone(parked));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut context) {
                drop(future);
                return output;
            }
            #[cfg(test)]
            if let Some(before_park) = BEFORE_PARK.with_borrow_mut(Option::take) {
                before_park();
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

#[cfg(test)]
mod future_custody_tests {
    use super::*;
    use std::cell::Cell;

    struct Callback<'a> {
        body: [u8; 4096],
        work: &'a crate::NativeMaterializationWork,
        dropped: &'a Cell<bool>,
        charged_at_drop: &'a Cell<bool>,
        panic: bool,
    }

    impl Future for Callback<'_> {
        type Output = Result<(), MountSourceError>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            assert!(
                self.work.live_allocation_bytes()
                    >= crate::NativeMaterializationWork::accounting_state_bytes()
                        + std::mem::size_of::<Self>() as u64
            );
            std::hint::black_box(&self.body);
            assert!(!self.panic, "callback unwind control");
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for Callback<'_> {
        fn drop(&mut self) {
            self.charged_at_drop.set(
                self.work.live_allocation_bytes()
                    >= crate::NativeMaterializationWork::accounting_state_bytes()
                        + std::mem::size_of::<Self>() as u64,
            );
            self.dropped.set(true);
        }
    }

    #[test]
    fn prepared_future_constructor_keeps_its_runtime_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let work =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        let prepared = runtime.prepare_native_wait(&work)?;
        runtime.wait_prepared(prepared, || {
            // Construct before returning the future: this requires the bridge's
            // runtime even when its caller has no Tokio context.
            let timer = tokio::time::sleep(std::time::Duration::ZERO);
            async move {
                // Construction itself needs the reactor. Do not register a
                // wake that the timer driver may still own after this returns.
                drop(timer);
                Ok::<_, MountSourceError>(())
            }
        })?;
        assert_eq!(
            work.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        Ok(())
    }

    #[test]
    fn retained_callback_waker_keeps_only_its_live_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let work =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        let retained = std::cell::RefCell::new(None);
        let baseline = work.live_allocation_bytes();
        let prepared = runtime.prepare_native_wait(&work)?;
        runtime.wait_prepared(prepared, || {
            std::future::poll_fn(|context| {
                retained.replace(Some(context.waker().clone()));
                Poll::Ready(Ok::<_, MountSourceError>(()))
            })
        })?;
        // Completion drops the future, but an external wake owner is still
        // alive. Its allocation must stay charged until that owner is dropped.
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + std::mem::size_of::<ParkedCallback>() as u64
        );
        drop(retained.into_inner());
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn prepared_future_is_admitted_before_construction() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let work = crate::NativeMaterializationWork::new(
            WorkBudget {
                // Exactly fund the original ledger and parked waiter, leaving
                // no bytes for the callback frame. Neither owner is free.
                peak_allocation_bytes: crate::NativeMaterializationWork::accounting_state_bytes()
                    + std::mem::size_of::<ParkedCallback>() as u64,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        let constructed = Cell::new(false);
        let dropped = Cell::new(false);
        let charged_at_drop = Cell::new(false);
        let prepared = runtime.prepare_native_wait(&work)?;
        let result = runtime.wait_prepared(prepared, || {
            constructed.set(true);
            Callback {
                body: [1; 4096],
                work: &work,
                dropped: &dropped,
                charged_at_drop: &charged_at_drop,
                panic: false,
            }
        });
        assert!(matches!(result, Err(MountSourceError::NativeWork(_))));
        assert!(!constructed.get());
        assert!(!dropped.get());
        assert_eq!(
            work.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        Ok(())
    }

    #[test]
    fn native_future_releases_after_an_unpolled_callback_is_dropped() {
        let work =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .unwrap();
        let dropped = Cell::new(false);
        let charged_at_drop = Cell::new(false);
        let future = native_future(&work, || Callback {
            body: [1; 4096],
            work: &work,
            dropped: &dropped,
            charged_at_drop: &charged_at_drop,
            panic: false,
        })
        .unwrap();
        assert!(!dropped.get());
        drop(future);
        assert!(dropped.get());
        assert!(charged_at_drop.get());
        assert_eq!(
            work.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
    }

    #[test]
    fn prepared_future_custody_outlives_completion_and_unwind()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        for panic in [false, true] {
            let work = crate::NativeMaterializationWork::new(
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .map_err(|(error, _)| error)?;
            let dropped = Cell::new(false);
            let charged_at_drop = Cell::new(false);
            let prepared = runtime.prepare_native_wait(&work)?;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.wait_prepared(prepared, || Callback {
                    body: [1; 4096],
                    work: &work,
                    dropped: &dropped,
                    charged_at_drop: &charged_at_drop,
                    panic,
                })
            }));
            if panic {
                assert!(result.is_err());
            } else {
                match result {
                    Ok(value) => value?,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            }
            assert!(dropped.get());
            assert!(charged_at_drop.get());
            assert_eq!(
                work.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes()
            );
        }
        Ok(())
    }
}
