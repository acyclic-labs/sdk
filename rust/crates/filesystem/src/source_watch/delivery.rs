use super::*;

impl Delivery {
    pub(super) fn new<W: SourceDemandReadWork>(
        sink: Arc<dyn HostChangeSink>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let original = work.native_source_work().map(|work| work.allocation_work());
        let controls = |work: &mut DemandWork| (work.budget, work.cancellation().clone());
        let (budget, cancellation) = work.with_original_leaf(controls);
        let ownership = SourcePayloadOwnership::new(work)?;
        Ok(Self {
            #[cfg(windows)]
            read_authority: None,
            sink,
            exact: AtomicBool::new(true),
            budget,
            cancellation,
            failure: std::sync::Mutex::new(None),
            #[cfg(target_os = "linux")]
            events: None,
            ownership,
            original,
        })
    }

    #[cfg(any(windows, target_os = "linux"))]
    pub(super) fn prepare_startup_failure<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<Option<Arc<SourceWatchStartupFailure>>, DemandError> {
        if self.original.is_none() {
            return Ok(None);
        }
        let ownership = SourcePayloadOwnership::new(work)?;
        let bytes = arc_control_bytes::<SourceWatchStartupFailure>();
        work.claim_allocation(bytes, 1, WorkCounters::UNCHARGED)?;
        ownership.retain(bytes, work)?;
        Ok(Some(Arc::new(SourceWatchStartupFailure {
            cause: std::sync::OnceLock::new(),
            watch: std::sync::Mutex::new(None),
            stopped: AtomicBool::new(false),
            _ownership: ownership,
        })))
    }

    pub(super) fn start<W: SourceDemandReadWork>(
        self,
        work: &mut W,
        open: impl FnOnce(Arc<Self>, &mut W) -> Result<NativeSourceWatch, DemandError>,
    ) -> Result<NativeSourceWatch, DemandError> {
        let bytes = arc_control_bytes::<Self>();
        work.claim_allocation(bytes, 1, WorkCounters::UNCHARGED)?;
        let delivery = Arc::new(self);
        delivery.ownership.retain(bytes, work)?;
        let result = open(delivery, work);
        if !work.is_native_original() {
            work.release(bytes)?;
        }
        result
    }

    pub(super) fn callback_work(&self) -> CallbackWork {
        #[cfg(windows)]
        if let Some(source) = &self.read_authority {
            return source.callback_work();
        }
        match &self.original {
            Some(original) => {
                CallbackWork::Original(crate::NativeMaterializationWork::clone(original))
            }
            None => CallbackWork::Ordinary(DemandWork::new(self.budget, &self.cancellation)),
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    pub(super) fn birth_matches<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        let Some(birth) = &self.original else {
            return false;
        };
        work.native_source_work()
            .is_some_and(|current| birth.shares_allocation_context(&current))
    }

    pub(super) fn failed_with_work<W: SourceDemandReadWork>(
        &self,
        error: DemandError,
        work: &mut W,
    ) {
        self.exact.store(false, Ordering::Release);
        let prior = if work.is_native_original() {
            None
        } else {
            Some(work.with_original_leaf(|work| work.work))
        };
        let mut failed = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failed.is_none() {
            *failed = Some((error, prior));
        }
    }

    pub(super) fn take_failure<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if let Some((error, prior)) = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            if let Some(prior) = prior {
                work.merge(prior)?;
            }
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn deliver<W: SourceDemandReadWork>(
        &self,
        changes: &[HostChange],
        work: &mut W,
    ) -> Result<(), DemandError> {
        if changes.is_empty() {
            return Ok(());
        }
        if let Some(mut native) = work.native_source_work() {
            return self.callback(|| {
                self.sink
                    .host_changed_with_native_work(changes, &mut native)
            });
        }
        work.with_ordinary_work(|work| self.callback(|| self.sink.host_changed(changes, work)))
            .unwrap_or(Err(DemandError::NativeWorkUnavailable))
    }

    #[cfg(target_os = "linux")]
    pub(super) fn invalidate<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        #[cfg(target_os = "linux")]
        if let Some(sink) = &self.events {
            if let Some(mut native) = work.native_source_work() {
                return self.callback(|| sink.invalidate_with_native_work(&mut native));
            }
            return work
                .with_ordinary_work(|work| self.callback(|| sink.invalidate_measured(work)))
                .unwrap_or(Err(DemandError::NativeWorkUnavailable));
        }
        self.deliver(&[HostChange::Everything], work)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn event<W: SourceDemandReadWork>(
        &self,
        relative: &std::path::Path,
        mask: u32,
        cookie: usize,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let sink = self
            .events
            .as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        if let Some(mut native) = work.native_source_work() {
            return self
                .callback(|| sink.event_with_native_work(relative, mask, cookie, &mut native));
        }
        work.with_ordinary_work(|work| {
            self.callback(|| sink.event_measured(relative, mask, cookie, work))
        })
        .unwrap_or(Err(DemandError::NativeWorkUnavailable))
    }

    #[cfg(windows)]
    pub(super) fn deliver_wide<W: SourceDemandReadWork>(
        &self,
        changes: &[HostChangeWide<'_>],
        work: &mut W,
    ) -> Result<(), DemandError> {
        if changes.is_empty() {
            return Ok(());
        }
        if let Some(mut native) = work.native_source_work() {
            return self.callback(|| {
                self.sink
                    .host_changed_wide_with_native_work(changes, &mut native)
            });
        }
        work.with_ordinary_work(|work| self.callback(|| self.sink.host_changed_wide(changes, work)))
            .unwrap_or(Err(DemandError::NativeWorkUnavailable))
    }

    /// The host stopped reporting changes exactly: report everything once, after which the watch is inexact for good.
    #[cfg(any(windows, target_os = "linux"))]
    pub(super) fn abandon(&self) {
        if self.exact.swap(false, Ordering::AcqRel) {
            let mut work = self.callback_work();
            if let Err(error) = self.deliver(&[HostChange::Everything], &mut work) {
                self.failed_with_work(error, &mut work);
            }
        }
    }

    pub(super) fn is_exact(&self) -> bool {
        self.exact.load(Ordering::Acquire)
    }
}
