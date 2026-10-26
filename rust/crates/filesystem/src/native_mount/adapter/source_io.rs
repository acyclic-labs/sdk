use super::*;
use crate::native_mount::retirement::SourceRetirementWait;

/// Retains a physical source operation until its last alias finishes.
pub type NativeSourceIoLease = Arc<CaptureProducer>;

impl CaptureProducer {
    pub(crate) fn original_work(&self) -> Option<&crate::NativeMaterializationWork> {
        self.original.as_ref()
    }
}

impl std::fmt::Debug for CaptureProducer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeSourceIoLease")
            .finish_non_exhaustive()
    }
}

/// One accepted physical source operation.
pub struct CaptureProducer {
    pub(super) gate: Arc<StdMutex<CaptureSourceGate>>,
    pub(super) quiesced: Arc<std::sync::Condvar>,
    pub(super) original: Option<crate::NativeMaterializationWork>,
    pub(super) _owner: Option<Arc<dyn Send + Sync>>,
    pub(super) _allocation: Option<crate::NativeAllocation>,
}

impl Drop for CaptureProducer {
    fn drop(&mut self) {
        let mut gate = self.gate.lock().unwrap_or_else(PoisonError::into_inner);
        gate.producers = {
            assert!(gate.producers > 0, "accepted physical source operation");
            gate.producers - 1
        };
        if gate.producers == 0 {
            self.quiesced.notify_all();
        }
    }
}

impl CaptureSourceGate {}

impl<A, O> CheckoutMountSource<A, O> {
    pub(super) fn with_capture_observer_dispatch<T>(&self, dispatch: impl FnOnce() -> T) -> T {
        crate::native_mount::lazy::with_capture_observer_dispatch(
            std::ptr::from_ref(self.capture_gate.as_ref()) as usize,
            dispatch,
        )
    }

    pub(in crate::native_mount) fn native_source_io_lease(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<NativeSourceIoLease, MountSourceError> {
        if self
            .capture_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancelled
        {
            return Err(MountSourceError::Stale);
        }
        let original = self
            .native_original
            .as_ref()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        let credit = self.reserve_capture_poll()?;
        let allocation = original
            .claim(
                std::mem::size_of::<CaptureProducer>() as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        let mut producer = self.capture_producer(credit)?;
        producer.original = self.native_io_original.clone();
        producer._owner = Some(owner);
        producer._allocation = Some(allocation);
        Ok(Arc::new(producer))
    }

    pub(super) fn capture_producer(
        &self,
        mut credit: Option<crate::materializer::NativeWorkReservation>,
    ) -> Result<CaptureProducer, MountSourceError> {
        let mut gate = self
            .capture_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if gate.cancelled {
            return Err(MountSourceError::Stale);
        }
        self.cancellation
            .check()
            .map_err(|_| MountSourceError::Stale)?;
        let producers = gate
            .producers
            .checked_add(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if let Some(credit) = &mut credit {
            self.cleanup_credit
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_mut()
                .ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?
                .0
                .absorb(credit)
                .map_err(MountSourceError::NativeWork)?;
        }
        gate.producers = producers;
        gate.ever_used = true;
        Ok(CaptureProducer {
            gate: Arc::clone(&self.capture_gate),
            quiesced: Arc::clone(&self.capture_quiesced),
            original: None,
            _owner: None,
            _allocation: None,
        })
    }

    pub(super) fn reserve_capture_poll(
        &self,
    ) -> Result<Option<crate::materializer::NativeWorkReservation>, MountSourceError> {
        self.native_original
            .as_ref()
            .map(|original| {
                original
                    .reserve_work(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })
                    .map_err(MountSourceError::NativeWork)
            })
            .transpose()
    }

    pub(in crate::native_mount) fn capture_retired(&self) -> bool {
        self.capture_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retired
    }

    pub(in crate::native_mount) fn cancel_cleanup(
        &self,
        wait: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError> {
        self.cancel_cleanup_with_join(wait, SourceRetirementWait::Caller)
    }

    pub(in crate::native_mount) fn cancel_cleanup_with_join(
        &self,
        wait: Option<&CancellationToken>,
        join: SourceRetirementWait,
    ) -> Result<(), MountSourceError> {
        if self.capture_retired() {
            return Ok(());
        }
        if let Some(wait) = wait {
            wait.check().map_err(|_| MountSourceError::Cancelled)?;
        }
        let prepared = self
            .cancellation
            .prepare_cancel_cleanup(wait)
            .map_err(MountSourceError::NativeWork)?;
        self.capture_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancelled = true;
        prepared.finish();
        if crate::native_mount::lazy::is_current_capture_observer_dispatch(std::ptr::from_ref(
            self.capture_gate.as_ref(),
        ) as usize)
        {
            return Err(MountSourceError::Stale);
        }
        loop {
            if let Some(wait) = wait {
                wait.check().map_err(|_| MountSourceError::Cancelled)?;
            }
            let mut gate = self
                .capture_gate
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if gate.retired {
                return Ok(());
            }
            if gate.producers == 0 {
                if self.native_original.is_some() {
                    let (poll_credit, final_credit) = self
                        .cleanup_credit
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                        .ok_or(MountSourceError::NativeWork(
                            crate::NativeTreeMaterializationError::InvalidLayout,
                        ))?;
                    final_credit
                        .record_completed(crate::WorkCounters {
                            items_examined: 1,
                            ..crate::WorkCounters::UNCHARGED
                        })
                        .map_err(MountSourceError::NativeWork)?;
                    drop(poll_credit);
                }
                gate.retired = true;
                self.capture_quiesced.notify_all();
                return Ok(());
            }
            if matches!(join, SourceRetirementWait::Physical) {
                #[cfg(test)]
                if let Some(observed) = gate.physical_cleanup_wait_observed.take() {
                    let _ = observed.send(());
                }
                // The original worker already owns this join. A wall-clock
                // poll is not another source item: wait for the last accepted
                // producer's actual drop, retaining the same source and ledger.
                gate = self
                    .capture_quiesced
                    .wait_while(gate, |gate| gate.producers != 0 && !gate.retired)
                    .unwrap_or_else(PoisonError::into_inner);
                drop(gate);
                continue;
            }
            if self.native_original.is_some() {
                let mut credit = self
                    .cleanup_credit
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let credit = credit.as_mut().ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?;
                credit
                    .0
                    .partition_work(
                        crate::WorkCounters {
                            items_examined: 1,
                            ..crate::WorkCounters::UNCHARGED
                        },
                        wait,
                    )
                    .map_err(MountSourceError::NativeWork)?
                    .record_completed(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })
                    .map_err(MountSourceError::NativeWork)?;
            }
            gate = self
                .capture_quiesced
                .wait_timeout(gate, Duration::from_millis(10))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
            drop(gate);
        }
    }
}
