use super::*;

impl NativeSourceWatch {
    /// Invalidates observations retained by this producer's original sink.
    pub(crate) fn delivery_lost(&self) {
        self.delivery().sink.delivery_lost();
    }

    /// Injects a terminal failure into this actual producer's original delivery.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn inject_delivery_failure(&self, error: DemandError) {
        let mut work = self.delivery().callback_work();
        self.delivery().failed_with_work(error, &mut work);
    }

    /// Whether this producer already observes the entire held subtree. This is
    /// registration coverage, not a delivery or restart-continuity guarantee.
    pub(crate) fn has_subtree_registration(&self) -> bool {
        #[cfg(target_os = "macos")]
        {
            // FSEvents registers a root and all its descendants in one stream.
            true
        }
        #[cfg(target_os = "linux")]
        {
            false
        }
        #[cfg(windows)]
        {
            self.registration_is_recursive()
        }
    }

    /// Retires physical resources on their birth credits, with the caller's wait.
    pub(crate) fn retire_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        if self.delivery().original.is_some() {
            let current = work
                .native_source_work()
                .ok_or(DemandError::NativeWorkUnavailable)?;
            self.stop_with_cleanup_wait(current.cancellation())
        } else {
            self.stop_with_work(work)
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
        &self,
        timeout: std::time::Duration,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        self.delivery().take_failure(work)?;
        self.fence_events_with_work(timeout, work)
    }

    #[cfg(windows)]
    pub(crate) fn start_held_with_sink_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
        complete: bool,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        Delivery::new(sink, work)?.start(work, |delivery, work| {
            Self::open_watch(
                directory.directory_handle(),
                delivery,
                false,
                Some(complete),
                true,
                work,
            )
        })
    }
    /// Starts reporting changes beneath the held root.
    pub(crate) fn start<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        Delivery::new(sink, work)?.start(work, |delivery, work| {
            Self::start_driver(directory, delivery, work)
        })
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn start_with_events_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
        events: Arc<dyn NativeEventSink>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let mut delivery = Delivery::new(sink, work)?;
        delivery.events = Some(events);
        delivery.start(work, |delivery, work| {
            Self::start_driver(directory, delivery, work)
        })
    }
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let delivery = self.delivery();
        let bytes = self.platform_owned_bytes()
            + arc_control_bytes::<Delivery>()
            + delivery.sink.logical_owned_bytes();
        #[cfg(target_os = "linux")]
        let bytes = bytes
            + delivery
                .events
                .as_ref()
                .map_or(0, |sink| sink.logical_owned_bytes());
        bytes
    }

    #[cfg(test)]
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn admit_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        directory: &std::path::Path,
        work: &mut W,
    ) -> Result<bool, crate::demand::DemandError> {
        let _ = directory;
        work.check()?;
        Ok(false)
    }

    /// Whether every change still reaches the sink.
    pub(crate) fn is_exact(&self) -> bool {
        self.delivery().is_exact()
    }
}
