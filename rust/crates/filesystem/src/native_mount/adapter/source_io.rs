use super::*;

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
    pub(super) signal: Arc<CaptureSourceSignal>,
    pub(super) original: Option<crate::NativeMaterializationWork>,
    pub(super) _owner: Option<Arc<dyn Send + Sync>>,
    pub(super) _allocation: Option<crate::NativeAllocation>,
}

impl Drop for CaptureProducer {
    fn drop(&mut self) {
        // Quiescence includes the accepted operation's retained custody. Their
        // destructors may lock the original ledger or block, so retire them
        // before publishing completion and without holding the source gate.
        drop(self.original.take());
        drop(self._allocation.take());
        drop(self._owner.take());
        let mut gate = self
            .signal
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        gate.producers = {
            assert!(gate.producers > 0, "accepted physical source operation");
            gate.producers - 1
        };
        if gate.producers == 0 {
            self.signal.quiesced.notify_all();
        }
    }
}

impl<A, O> CheckoutMountSource<A, O> {
    pub(super) fn with_capture_observer_dispatch<T>(&self, dispatch: impl FnOnce() -> T) -> T {
        crate::native_mount::lazy::with_capture_observer_dispatch(
            std::ptr::from_ref(&self.capture_signal.gate) as usize,
            dispatch,
        )
    }

    pub(in crate::native_mount) fn native_source_io_lease(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<NativeSourceIoLease, MountSourceError> {
        if self
            .capture_signal
            .gate
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
        let allocation = original
            .claim(
                std::mem::size_of::<CaptureProducer>() as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        let mut producer = self.capture_producer()?;
        producer.original = self.native_io_original.clone();
        producer._owner = Some(owner);
        producer._allocation = Some(allocation);
        Ok(Arc::new(producer))
    }

    pub(super) fn capture_producer(&self) -> Result<CaptureProducer, MountSourceError> {
        let mut gate = self
            .capture_signal
            .gate
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
        if let Some(original) = &self.native_original {
            original
                .charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })
                .map_err(MountSourceError::NativeWork)?;
        }
        gate.producers = producers;
        gate.ever_used = true;
        Ok(CaptureProducer {
            signal: Arc::clone(&self.capture_signal),
            original: None,
            _owner: None,
            _allocation: None,
        })
    }

    pub(in crate::native_mount) fn capture_retired(&self) -> bool {
        self.capture_signal
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retired
    }

    pub(in crate::native_mount) fn cancel_cleanup(
        &self,
        wait: Option<&CancellationToken>,
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
        self.capture_signal
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancelled = true;
        prepared.finish();
        if crate::native_mount::lazy::is_current_capture_observer_dispatch(std::ptr::from_ref(
            &self.capture_signal.gate,
        ) as usize)
        {
            return Err(MountSourceError::Stale);
        }
        let mut registration = None;
        loop {
            if let Some(wait) = wait {
                wait.check().map_err(|_| MountSourceError::Cancelled)?;
            }
            let mut gate = self
                .capture_signal
                .gate
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if gate.retired {
                return Ok(());
            }
            if gate.producers == 0 {
                if self.native_original.is_some() {
                    let final_credit = self
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
                }
                gate.retired = true;
                self.capture_signal.quiesced.notify_all();
                return Ok(());
            }
            if registration.is_none()
                && let Some(wait) = wait
            {
                let mut cancelled = match &self.native_original {
                    Some(original) => wait
                        .prepare_cancelled_with_native_work(original)
                        .map_err(|cause| MountSourceError::NativeWork(cause.into_native()))?,
                    None => wait.cancelled(),
                };
                let wake = Waker::from(Arc::clone(&self.capture_signal));
                if Pin::new(&mut cancelled)
                    .poll(&mut Context::from_waker(&wake))
                    .is_ready()
                {
                    return Err(MountSourceError::Cancelled);
                }
                registration = Some(cancelled);
            }
            #[cfg(test)]
            if let Some(observed) = gate.physical_cleanup_wait_observed.take() {
                let _ = observed.send(());
            }
            // Completion and cancellation take this same gate before notifying.
            // Condvar's atomic unlock-and-wait therefore cannot lose either event.
            gate = self
                .capture_signal
                .quiesced
                .wait_while(gate, |gate| {
                    gate.producers != 0
                        && !gate.retired
                        && wait.is_none_or(|wait| !wait.is_cancelled())
                })
                .unwrap_or_else(PoisonError::into_inner);
            drop(gate);
        }
    }
}

impl Wake for CaptureSourceSignal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _gate = self.gate.lock().unwrap_or_else(PoisonError::into_inner);
        self.quiesced.notify_all();
    }
}
