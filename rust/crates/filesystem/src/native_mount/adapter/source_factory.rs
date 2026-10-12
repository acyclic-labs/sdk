use super::*;

impl<A, O> CheckoutMountSource<A, O> {
    /// Binds callback authentication to this source.
    pub fn with_native_callback_guard(mut self, guard: Arc<dyn crate::native_mount::NativeMountCallbackGuard>) -> Self {
        self.native_callback_guard = Some(guard);
        self
    }

    pub(crate) fn new_at_meter(
        checkout: Arc<SharedCheckout<A, O>>, config: VolumeConfig, root: NamespacePath,
        root_allocation: crate::NativeAllocation, original: &crate::NativeMaterializationWork,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, NativeMountError> {
        let allocation = root_allocation;
        let root = root;
        let work = original.allocation_work();
        if !allocation.work().shares_ledger(&work) || !allocation.work().shares_resource_allowance(&work)
            || allocation.bytes() != root.retained_bytes()
            || native_original.is_some_and(|native| !native.shares_ledger(&work) || !native.shares_resource_allowance(&work))
        {
            return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout).into());
        }
        work.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        if !profile_is_native(config.profile) {
            return Err(NativeMountError::ProfileUnavailable {
                profile: profile_name(config.profile), platform: std::env::consts::OS,
            });
        }
        let runtime = CallbackRuntime::existing()?;
        let cancellation = CancellationToken::new_with_native_work(&work).map_err(MountSourceError::NativeWork)?;
        let gate_allocation = work.claim_allocation(std::mem::size_of::<StdMutex<CaptureSourceGate>>() as u64,
            1, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let quiesced_allocation = work.claim_allocation(std::mem::size_of::<std::sync::Condvar>() as u64,
            1, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let mut allocation = allocation;
        allocation.absorb(&mut { gate_allocation }).map_err(MountSourceError::NativeWork)?;
        allocation.absorb(&mut { quiesced_allocation }).map_err(MountSourceError::NativeWork)?;
        let mut cleanup_credit = work.reserve_work(crate::WorkCounters {
            items_examined: 2, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)?;
        let final_credit = cleanup_credit.partition_work(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)?;
        Ok(Self {
            checkout, root, limits: config.limits, profile: config.profile,
            folds_names: config.case_sensitivity == CaseSensitivity::ProfileFolded
                && matches!(config.profile, FilesystemProfile::Portable | FilesystemProfile::Browser),
            cancellation, capture_gate: Arc::new(StdMutex::new(CaptureSourceGate::default())),
            capture_quiesced: Arc::new(std::sync::Condvar::new()), runtime,
            native_io_original: native_original.cloned(), native_original: Some(work), native_callback_guard: None,
            cleanup_credit: StdMutex::new(Some(cleanup_credit)), cleanup_final_credit: StdMutex::new(Some(final_credit)),
            _namespace_allocation: Some(allocation), _source_allocation: None,
        })
    }

    pub(crate) fn into_arc_meter(mut self, original: &crate::NativeMaterializationWork)
        -> Result<Arc<Self>, NativeMountError>
    where Self: Send + Sync + 'static {
        let stored = self.native_original.as_ref().ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout))?;
        if !stored.shares_ledger(original) || !stored.shares_resource_allowance(original) {
            return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout).into());
        }
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        self._source_allocation = Some(original.claim_allocation(std::mem::size_of::<Self>() as u64,
            1, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?);
        Ok(Arc::new(self))
    }
}
