use super::*;
mod start;

pub(crate) trait StreamTerminal: Send + Sync {
    fn stream_stopped(&self);
}

/// One running `FSEvents` stream over some paths, delivering file events as
/// they happen on its own serial queue. Dropping it stops it, after any
/// batch in delivery.
pub(crate) struct EventStream {
    api: &'static Api,
    pub(super) stream: FsEventStreamRef,
    pub(super) queue: DispatchQueue,
    context: *const StreamContext,
    terminal: Option<Arc<dyn StreamTerminal>>,
    started: bool,
    scheduled: bool,
    owned_bytes: u64,
    ownership: crate::demand::SourcePayloadOwnership,
    cleanup: (u64, u64),
    pub(super) birth: Option<crate::NativeMaterializationWork>,
}

// SAFETY: FSEvents streams and dispatch queues may be stopped and released
// from another thread; exclusive custody serializes every teardown.
unsafe impl Send for EventStream {}
// SAFETY: shared references do not mutate the native resource custody.
unsafe impl Sync for EventStream {}

impl EventStream {
    /// Returns the final queued event to await outside the stream mutex.
    /// A callback cannot wait on its own serial queue.
    pub(crate) fn flush_target<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<Option<u64>, DemandError> {
        work.check()?;
        if !self.started || ACTIVE_STREAM.with(|active| active.get() == self.stream) {
            return Ok(None);
        }
        work.started_io(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        // SAFETY: the caller holds custody of this started stream. This call
        // queues a flush without waiting for or running its callbacks here.
        Ok(Some(unsafe { (self.api.stream_flush_async)(self.stream) }))
    }

    /// Initializes process-global framework services outside finite source work.
    pub(crate) fn initialize_services() -> Result<(), DemandError> {
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Api::get(&mut work).map(|_| ())
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
        self.stream.is_null() && self.queue.is_null()
    }

    #[cfg(test)]
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.owned_bytes
    }

    pub(crate) fn set_terminal(&mut self, terminal: Arc<dyn StreamTerminal>) {
        self.terminal = Some(terminal);
    }

    pub(super) fn cleanup_work(&self) -> WorkCounters {
        WorkCounters {
            backend_write_operations: u64::from(!self.stream.is_null())
                + u64::from(self.started)
                + u64::from(self.scheduled)
                + u64::from(!self.queue.is_null()),
            backend_read_operations: u64::from(!self.queue.is_null()),
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
    /// a recording error. Invalidation removes the producer before the serial
    /// queue barrier establishes that its last callback has returned.
    pub(crate) fn cleanup_admitted<W: SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = Ok(());
        if self.started {
            // SAFETY: stop is valid only after the actual successful start.
            unsafe { (self.api.stream_stop)(self.stream) };
            self.started = false;
            result = result.and(self.record_cleanup(work, 0, 1));
        }
        if self.scheduled {
            // SAFETY: this actual stream was scheduled on its live queue.
            unsafe { (self.api.stream_invalidate)(self.stream) };
            self.scheduled = false;
            result = result.and(self.record_cleanup(work, 0, 1));
        }
        if !self.stream.is_null() && ACTIVE_STREAM.with(|active| active.get() == self.stream) {
            return result.and(self.defer_terminal(work));
        }
        if !self.queue.is_null() {
            // SAFETY: this original serial queue is live. No callback-owned
            // state mutex is held; the barrier waits for its pending callbacks.
            unsafe { dispatch_sync_f(self.queue, std::ptr::null_mut(), drain) };
            result = result.and(self.record_cleanup(work, 1, 0));
        }
        result = result.and(self.release_drained_handles(work));
        result = result.and(self.retire_drained_custody());
        if let Some(terminal) = self.terminal.take() {
            terminal.stream_stopped();
        }
        result
    }

    fn release_drained_handles<W: SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = Ok(());
        if !self.stream.is_null() {
            // SAFETY: invalidated and drained; release also frees the context.
            unsafe { (self.api.stream_release)(self.stream) };
            self.stream = std::ptr::null_mut();
            result = result.and(self.record_cleanup(work, 0, 1));
        }
        if !self.queue.is_null() {
            // SAFETY: no live stream or callback needs this queue reference.
            unsafe { dispatch_release(self.queue) };
            self.queue = std::ptr::null_mut();
            result = result.and(self.record_cleanup(work, 0, 1));
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

    fn defer_terminal<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        let context = self.context;
        let queue = self.queue;
        let api = self.api;
        // SAFETY: this is the currently executing callback's live original
        // Arc context. Its serial queue is the slot's sole writer/consumer.
        let slot = unsafe { &mut *(*context).terminal.get() };
        assert!(
            slot.is_none(),
            "one original stream schedules one terminal suffix"
        );
        let recorded = self.record_cleanup(work, 0, 1);
        // SAFETY: the stream still owns its raw Arc reference. This allocation-
        // free clone keeps the context alive through native release in the thunk.
        unsafe { Arc::increment_strong_count(context) };
        *slot = Some(std::mem::replace(
            self,
            Self {
                api,
                stream: std::ptr::null_mut(),
                queue: std::ptr::null_mut(),
                context: std::ptr::null(),
                terminal: None,
                started: false,
                scheduled: false,
                owned_bytes: 0,
                ownership: Default::default(),
                cleanup: (0, 0),
                birth: None,
            },
        ));
        // SAFETY: original serial queue + admitted scheduling stage. The same
        // queue consumes this sole raw Arc reference after prior callbacks drain.
        unsafe { dispatch_async_f(queue, context.cast_mut().cast(), terminal_suffix) };
        recorded
    }

    pub(super) fn finish_deferred<W: SourceDemandReadWork>(
        mut self,
        context: Arc<StreamContext>,
        work: &mut W,
    ) {
        let terminal = self.terminal.take();
        let result = self.release_drained_handles(work);
        // The stream's native release consumed its original context reference;
        // our queued reference is last, so handler payloads physically drop now.
        drop(context);
        let result = result.and(self.retire_drained_custody());
        if let Err(error) = result {
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
        if self.stream.is_null() && self.queue.is_null() {
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
