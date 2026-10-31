use super::*;
mod start;

pub(crate) trait StreamTerminal: Send + Sync {
    fn stream_stopped(&self);
}

#[cfg(test)]
pub(crate) struct DeliveryFence(&'static Hub);

#[cfg(test)]
impl DeliveryFence {
    /// Settles the original callback queue, including deferred terminal drops.
    pub(crate) fn wait(self) -> Result<(), DemandError> {
        if on_queue() {
            return Err(DemandError::WorkerUnavailable);
        }
        self.0.drain();
        Ok(())
    }
}

/// One registration of some roots on the process's shared `FSEvents` hub,
/// delivering file events beneath them on the hub's serial queue. Dropping
/// it unregisters it, after any batch in delivery.
pub(crate) struct EventStream {
    hub: &'static Hub,
    pub(super) registration: Option<Arc<Registration>>,
    terminal: Option<Arc<dyn StreamTerminal>>,
    owned_bytes: u64,
    ownership: crate::demand::SourcePayloadOwnership,
    cleanup: (u64, u64),
    pub(super) birth: Option<crate::NativeMaterializationWork>,
}

impl EventStream {
    #[cfg(test)]
    pub(crate) fn delivery_fence(&self) -> DeliveryFence {
        DeliveryFence(self.hub)
    }

    /// Initializes process-global framework services outside finite source work.
    pub(crate) fn initialize_services() -> notify::Result<()> {
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Hub::get(&mut work).map(|_| ()).map_err(notify_error)
    }
    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        if !self.birth_work(work) {
            work.admit(self.cleanup_work())?;
        }
        self.cleanup_admitted(work)
    }

    /// Audits the original withheld physical suffix without admitting any new
    /// work or consulting cancellation of the root that funded native birth.
    pub(crate) fn ensure_cleanup_wait_available<W: SourceDemandReadWork>(
        &self,
        work: &W,
    ) -> Result<(), DemandError> {
        let required = self.cleanup_work();
        if !self.birth_work(work)
            || self.cleanup.0 < required.backend_read_operations
            || self.cleanup.1 < required.backend_write_operations
        {
            return Err(DemandError::NativeWorkUnavailable);
        }
        Ok(())
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.registration.is_none()
    }

    #[cfg(test)]
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.owned_bytes
    }

    pub(crate) fn set_terminal(&mut self, terminal: Arc<dyn StreamTerminal>) {
        self.terminal = Some(terminal);
    }

    /// Unregistering, then the queue barrier after which no batch runs.
    pub(super) fn cleanup_work(&self) -> WorkCounters {
        let registered = u64::from(self.registration.is_some());
        WorkCounters {
            backend_write_operations: registered,
            backend_read_operations: registered,
            ..WorkCounters::default()
        }
    }

    fn birth_work<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        work.native_source_work().is_some_and(|current| {
            self.birth.as_ref().is_some_and(|birth| {
                Arc::ptr_eq(&birth.state, &current.state)
                    && match (birth.resource.as_ref(), current.resource.as_ref()) {
                        (None, None) => true,
                        (Some(birth), Some(current)) => Arc::ptr_eq(birth, current),
                        _ => false,
                    }
            })
        })
    }

    fn record_cleanup<W: SourceDemandReadWork>(
        &mut self,
        work: &mut W,
        reads: u64,
        writes: u64,
    ) -> Result<(), DemandError> {
        let delta = WorkCounters {
            backend_read_operations: reads,
            backend_write_operations: writes,
            ..WorkCounters::default()
        };
        if !self.birth_work(work) {
            return work.record(delta);
        }
        if self.cleanup.0 < reads || self.cleanup.1 < writes {
            return Err(DemandError::InvalidRequest);
        }
        self.cleanup.0 -= reads;
        self.cleanup.1 -= writes;
        work.with_original_leaf(|work| crate::source_watch::record_cleanup_credits(work, delta))
    }

    /// Finishes every admitted native operation, even after cancellation or
    /// a recording error. Unregistering removes the routes before the serial
    /// queue barrier establishes that the handler's last batch has returned.
    pub(crate) fn cleanup_admitted<W: SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let Some(registration) = self.registration.take() else {
            return Ok(());
        };
        self.hub.unregister(&registration);
        let mut result = self.record_cleanup(work, 0, 1);
        if on_queue() {
            return result.and(self.defer_terminal(registration, work));
        }
        self.hub.drain();
        result = result.and(self.record_cleanup(work, 1, 0));
        drop(registration);
        result = result.and(self.retire_drained_custody());
        if let Some(terminal) = self.terminal.take() {
            terminal.stream_stopped();
        }
        result
    }

    fn retire_drained_custody(&mut self) -> Result<(), DemandError> {
        let mut result = Ok(());
        result = result.and(self.ownership.retire(self.owned_bytes));
        self.owned_bytes = 0;
        if let Some(birth) = &self.birth {
            let (reads, writes) = std::mem::take(&mut self.cleanup);
            crate::NativeMaterializationWork::clone(birth).with_original_leaf(|work| {
                crate::source_watch::cancel_cleanup_credits(
                    work,
                    WorkCounters {
                        backend_read_operations: reads,
                        backend_write_operations: writes,
                        ..WorkCounters::UNCHARGED
                    },
                );
            });
        }
        result
    }

    fn defer_terminal<W: SourceDemandReadWork>(
        &mut self,
        registration: Arc<Registration>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let hub = self.hub;
        // SAFETY: this runs on the hub's serial queue, the slot's sole
        // writer/consumer.
        let slot = unsafe { &mut *registration.terminal.get() };
        assert!(
            slot.is_none(),
            "one registration schedules one terminal suffix"
        );
        let recorded = self.record_cleanup(work, 0, 1);
        *slot = Some(std::mem::replace(
            self,
            Self {
                hub,
                registration: None,
                terminal: None,
                owned_bytes: 0,
                ownership: Default::default(),
                cleanup: (0, 0),
                birth: None,
            },
        ));
        // SAFETY: the same serial queue consumes this sole raw Arc reference
        // after the batch in delivery returns.
        unsafe {
            dispatch_async_f(
                hub.queue(),
                Arc::into_raw(registration).cast_mut().cast(),
                terminal_suffix,
            );
        }
        recorded
    }

    pub(super) fn finish_deferred<W: SourceDemandReadWork>(
        mut self,
        registration: Arc<Registration>,
        work: &mut W,
    ) {
        let terminal = self.terminal.take();
        // The routes and the batch are gone; this is the last reference, so
        // handler payloads physically drop now.
        drop(registration);
        if let Err(error) = self.retire_drained_custody() {
            work.with_original_leaf(|work| work.record_native_io_failure(error));
        }
        if let Some(terminal) = terminal {
            terminal.stream_stopped();
        }
        // The original custody and birth keeper outlive every physical suffix.
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if self.registration.is_none() {
            return;
        }
        if let Some(birth) = self.birth.clone() {
            let mut work = crate::NativeMaterializationWork::clone(&birth);
            let _ = self.cleanup_admitted(&mut work);
            return;
        }
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        if let Err(error) = self.stop_with_work(&mut work) {
            work.record_native_io_failure(error);
            work.poison_native_original_work();
        }
    }
}
