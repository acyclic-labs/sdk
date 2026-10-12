use super::*;

/// Retains a physical source operation until its last alias finishes.
#[derive(Clone)]
pub struct NativeSourceIoLease {
    inner: Arc<CaptureProducer>,
}

impl NativeSourceIoLease {
    pub(crate) fn original_work(&self) -> Option<&crate::NativeMaterializationWork> {
        self.inner.original.as_ref()
    }
}

impl std::fmt::Debug for NativeSourceIoLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeSourceIoLease").finish_non_exhaustive()
    }
}

pub(super) struct CaptureProducer {
    pub(super) gate: Arc<StdMutex<CaptureSourceGate>>,
    pub(super) quiesced: Arc<std::sync::Condvar>,
    pub(super) original: Option<crate::NativeMaterializationWork>,
    pub(super) _owner: Option<Arc<dyn Send + Sync>>,
    pub(super) _allocation: Option<crate::NativeAllocation>,
}

impl Drop for CaptureProducer {
    fn drop(&mut self) {
        let mut gate = self.gate.lock().unwrap_or_else(PoisonError::into_inner);
        gate.producers = gate.producers.checked_sub(1).expect("accepted physical source operation");
        if gate.producers == 0 { self.quiesced.notify_all(); }
    }
}

impl CaptureSourceGate {
    pub(super) fn fresh_for_namespace(&self) -> bool {
        !self.namespace_bound && !self.ever_used && !self.cancelled && !self.retired && self.producers == 0
    }
}

#[cfg(windows)]
pub(in crate::native_mount) fn namespace_gate_is_fresh(lineage: &crate::native_mount::NativeMountSourceLineage<'_>) -> bool {
    lineage.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).fresh_for_namespace()
        && !lineage.capture_cancellation.is_cancelled()
}

impl<A, O> CheckoutMountSource<A, O> {
    pub(crate) fn native_source_io_lease(self: &Arc<Self>) -> Result<NativeSourceIoLease, MountSourceError>
    where Self: Send + Sync + 'static {
        self.native_source_io_lease_for_owner(Arc::clone(self) as Arc<dyn Send + Sync>)
    }

    pub(in crate::native_mount) fn native_source_io_lease_for_owner(
        &self, owner: Arc<dyn Send + Sync>,
    ) -> Result<NativeSourceIoLease, MountSourceError> {
        if self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).cancelled {
            return Err(MountSourceError::Stale);
        }
        let original = self.native_original.as_ref().ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout))?;
        let mut credit = self.reserve_capture_poll()?;
        let allocation = original.claim_allocation(std::mem::size_of::<CaptureProducer>() as u64, 1,
            crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
        if gate.cancelled { return Err(MountSourceError::Stale); }
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let producers = gate.producers.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.accept_capture_poll(&mut credit)?;
        gate.producers = producers;
        gate.ever_used = true;
        let inner = Arc::new(CaptureProducer {
            gate: Arc::clone(&self.capture_gate), quiesced: Arc::clone(&self.capture_quiesced),
            original: self.native_io_original.clone(), _owner: Some(owner), _allocation: Some(allocation),
        });
        Ok(NativeSourceIoLease { inner })
    }

    pub(super) fn reserve_capture_poll(&self) -> Result<Option<crate::materializer::NativeWorkReservation>, MountSourceError> {
        self.native_original.as_ref().map(|original| original.reserve_work(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)).transpose()
    }

    pub(super) fn accept_capture_poll(&self, credit: &mut Option<crate::materializer::NativeWorkReservation>)
        -> Result<(), MountSourceError>
    {
        if let Some(credit) = credit {
            self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner).as_mut()
                .ok_or(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))?
                .absorb(credit).map_err(MountSourceError::NativeWork)?;
        }
        Ok(())
    }

    pub(in crate::native_mount) fn capture_retired(&self) -> bool {
        self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).retired
    }

    pub(in crate::native_mount) fn cancel_cleanup(&self, wait: Option<&CancellationToken>)
        -> Result<(), MountSourceError>
    {
        if self.capture_retired() { return Ok(()); }
        if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
        let prepared = match wait {
            Some(wait) => self.cancellation.prepare_cancel_with_cleanup_wait(wait),
            None => self.cancellation.prepare_cancel_accepted_cleanup(),
        }.map_err(MountSourceError::NativeWork)?;
        self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).cancelled = true;
        prepared.finish();
        if crate::native_mount::lazy::is_current_capture_observer_dispatch(
            std::ptr::from_ref(self.capture_gate.as_ref()) as usize,
        ) { return Err(MountSourceError::Stale); }
        loop {
            if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.retired { return Ok(()); }
            if gate.producers == 0 {
                if self.native_original.is_some() {
                    let mut credit = self.cleanup_final_credit.lock().unwrap_or_else(PoisonError::into_inner)
                        .take().ok_or(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))?;
                    credit.record_completed(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                        .map_err(MountSourceError::NativeWork)?;
                }
                self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner).take();
                gate.retired = true;
                self.capture_quiesced.notify_all();
                return Ok(());
            }
            if self.native_original.is_some() {
                let mut credit = self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner);
                let credit = credit.as_mut().ok_or(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))?;
                credit.loan(wait).admit_completed(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(MountSourceError::NativeWork)?.record_completed(crate::WorkCounters {
                        items_examined: 1, ..crate::WorkCounters::UNCHARGED
                    }).map_err(MountSourceError::NativeWork)?;
            }
            gate = self.capture_quiesced.wait_timeout(gate, Duration::from_millis(10))
                .unwrap_or_else(PoisonError::into_inner).0;
            drop(gate);
        }
    }
}
