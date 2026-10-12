use super::*;

/// Authentic source-owner authorization checked before a native SDK callback.
pub trait NativeMountCallbackGuard: Send + Sync {
    /// Admits the real verification future on the callback's original context.
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError>;
    /// Verifies current authored namespace WRITE permission on the same original task owner, fence, exact source and purpose.
    fn check_write<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError>;
}

/// One directly erased authorization future and its original funded custody.
pub struct NativeMountCallbackFuture<'a> {
    pub(super) future: std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a>,
    >,
    pub(super) _custody: crate::NativeAllocation,
}

impl<'a> NativeMountCallbackFuture<'a> {
    /// Admits the concrete boxed future before invoking its builder.
    pub fn new<F, B>(work: &NativeMaterializationWork, build: B) -> Result<Self, NativeMountError>
    where
        F: std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a,
        B: FnOnce() -> F,
    {
        let future_bytes = std::mem::size_of::<F>();
        let allocation = work.claim(
            future_bytes as u64,
            u64::from(future_bytes != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        let future = Box::pin(build());
        Ok(Self {
            future,
            _custody: allocation,
        })
    }

    /// Preallocates the exact typed failure slot before authorization starts.
    pub fn new_guarded<E, F, B>(
        work: &NativeMaterializationWork,
        build: B,
    ) -> Result<Self, NativeMountError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: std::future::Future<Output = Result<(), E>> + Send + 'a,
        B: FnOnce() -> F + Send + 'a,
    {
        let failure = callback_failure_slot::<E>(work)?;
        Self::new(work, move || async move {
            match build().await {
                Ok(()) => {
                    drop(failure);
                    Ok(())
                }
                Err(error) => Err(callback_failure(failure, error)),
            }
        })
    }
    /// Reuses an already funded native denial rather than nesting its custody inside another cause slot.
    pub(crate) fn new_native_guarded<F, B>(
        work: &NativeMaterializationWork,
        build: B,
    ) -> Result<Self, NativeMountError>
    where
        F: std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a,
        B: FnOnce() -> F + Send + 'a,
    {
        let failure = callback_failure_slot::<NativeMountError>(work)?;
        Self::new(work, move || async move {
            match build().await {
                Ok(()) => Ok(()),
                Err(error @ NativeMountError::CallbackDenied { .. }) => Err(error),
                Err(error) => Err(callback_failure(failure, error)),
            }
        })
    }
}

impl std::future::Future for NativeMountCallbackFuture<'_> {
    type Output = Result<(), NativeMountError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(context)
    }
}

pub(super) fn callback_failure_slot<E>(
    work: &NativeMaterializationWork,
) -> Result<(Box<std::mem::MaybeUninit<E>>, crate::NativeAllocation), NativeMountError> {
    let slot_bytes = std::mem::size_of::<E>();
    let allocation = work.claim(
        slot_bytes as u64,
        u64::from(slot_bytes != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    // Tuple fields retire the boxed slot before its allocation.
    Ok((Box::new(std::mem::MaybeUninit::uninit()), allocation))
}

pub(super) fn callback_failure<E: std::error::Error + Send + Sync + 'static>(
    (source, custody): (Box<std::mem::MaybeUninit<E>>, crate::NativeAllocation),
    error: E,
) -> NativeMountError {
    let source = Box::write(source, error);
    NativeMountError::CallbackDenied { source, custody }
}

/// One existing physical driver's accounting activation, not task authority.
#[derive(Default)]
pub(crate) struct NativeMountMeterActivation {
    pub(super) meter: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
    pub(super) resource_allowance:
        Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
    pub(super) capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    pub(super) invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
    pub(super) execution: Option<std::sync::Weak<crate::materializer::NativeSourceExecutionView>>,
    pub(super) depth: u64,
    pub(super) generation: u64,
}

impl NativeMountMeterActivation {
    pub(crate) fn upgrade(&self) -> Option<NativeMaterializationWork> {
        let meter = self.meter.upgrade()?;
        let resource_allowance = match self.resource_allowance.as_ref() {
            Some(resource) => Some(resource.upgrade()?),
            None => None,
        };
        let invocation = match self.invocation.as_ref() {
            Some(invocation) => Some(invocation.upgrade()?),
            None => None,
        };
        let execution = match self.execution.as_ref() {
            Some(execution) => Some(execution.upgrade()?),
            None => None,
        };
        let mut work = NativeMaterializationWork::from_meter(meter);
        work.resource = resource_allowance;
        work.capture_origin = self.capture_origin.clone();
        work.invocation = invocation;
        work.execution = execution;
        Some(work)
    }

    pub(super) fn matches(&self, work: &NativeMaterializationWork) -> bool {
        self.meter.as_ptr() == Arc::as_ptr(&work.state)
            && self
                .resource_allowance
                .as_ref()
                .map(std::sync::Weak::as_ptr)
                == work.resource.as_ref().map(Arc::as_ptr)
            && self.capture_origin.as_ref().map(std::sync::Weak::as_ptr)
                == work.capture_origin.as_ref().map(std::sync::Weak::as_ptr)
            && self.execution.as_ref().map(std::sync::Weak::as_ptr)
                == work.execution.as_ref().map(Arc::as_ptr)
            && self.invocation.as_ref().map(std::sync::Weak::as_ptr)
                == work.invocation.as_ref().map(Arc::as_ptr)
    }

    pub(crate) fn enter(
        &mut self,
        work: &NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        if self.depth != 0 && !self.matches(work) {
            return Err(NativeMountError::ControlPending);
        }
        let depth = self
            .depth
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        if self.depth == 0 {
            let generation = self
                .generation
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            self.meter = Arc::downgrade(&work.state);
            self.resource_allowance = work.resource.as_ref().map(Arc::downgrade);
            self.capture_origin = work.capture_origin.clone();
            self.invocation = work.invocation.as_ref().map(Arc::downgrade);
            self.execution = work.execution.as_ref().map(Arc::downgrade);
            self.generation = generation;
        }
        self.depth = depth;
        Ok(self.generation)
    }

    pub(crate) fn leave(&mut self, work: &NativeMaterializationWork, generation: u64) {
        assert!(
            self.depth != 0 && self.generation == generation && self.matches(work),
            "native accounting scope must retire its own activation"
        );
        self.depth -= 1;
        if self.depth == 0 {
            self.meter = std::sync::Weak::new();
            self.resource_allowance = None;
            self.capture_origin = None;
            self.invocation = None;
            self.execution = None;
        }
    }
}

/// Executes one installed SDK handler request with the original constructor's copied full grants and cancellation, through the existing native queue.
pub(crate) fn run_native_mount_handler<T>(
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    operation: impl FnOnce(&NativeMaterializationWork) -> Result<T, NativeMountError>,
) -> Result<crate::OperationReceipt<T>, (NativeMountError, crate::WorkCounters)> {
    let work = crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| (cause.into(), work))?;
    let result = run_native_mount_handler_on_meter(&work, operation);
    match result {
        Ok(value) => Ok(crate::OperationReceipt {
            value,
            work: work.receipt(),
        }),
        Err(cause) => Err((cause, work.receipt())),
    }
}

/// Startup/revalidation handlers use the already operative invocation.
pub(crate) fn run_native_mount_handler_on_meter<T>(
    work: &NativeMaterializationWork,
    operation: impl FnOnce(&NativeMaterializationWork) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(work))).unwrap_or_else(
        |panic| {
            Err(NativeMountError::DriverPanicked {
                operation: "SDK callback",
                panic: std::sync::Mutex::new(panic),
            })
        },
    )
}

/// Borrowed stock-source consistency data, not namespace or task authority.
pub struct NativeMountSourceLineage<'a> {
    pub(super) owner: Arc<dyn std::any::Any + Send + Sync>,
    pub(super) root: &'a crate::kernel::NamespacePath,
    pub(super) native_original: Option<&'a crate::NativeMaterializationWork>,
    pub(super) lazy_owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    pub(super) lazy_root: Option<&'a str>,
    pub(super) limits: crate::VolumeLimits,
    pub(super) capture_gate: &'a std::sync::Mutex<adapter::CaptureSourceGate>,
    pub(super) capture_cancellation: &'a crate::CancellationToken,
}

#[cfg(windows)]
pub(super) struct NativeMountNamespaceBinding {
    pub(super) owner: Arc<dyn std::any::Any + Send + Sync>,
    pub(super) root: crate::kernel::NamespacePath,
    pub(super) native_original: Option<crate::NativeMaterializationWork>,
    pub(super) lazy_owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    pub(super) lazy_root: Option<String>,
    pub(super) _custody: crate::NativeAllocation,
}

#[cfg(windows)]
impl NativeMountNamespaceBinding {
    pub(super) fn matches(&self, next: &NativeMountSourceLineage<'_>) -> bool {
        Arc::ptr_eq(&self.owner, &next.owner)
            && self.root == *next.root
            && match (&self.native_original, next.native_original) {
                (Some(old), Some(new)) => {
                    old.shares_ledger(new) && old.shares_resource_allowance(new)
                }
                (None, None) => true,
                _ => false,
            }
            && match (&self.lazy_owner, &next.lazy_owner) {
                (Some(old), Some(new)) => Arc::ptr_eq(old, new),
                (None, None) => true,
                _ => false,
            }
            && self.lazy_root.as_deref() == next.lazy_root
    }
}
