use super::*;

impl NativeSourceWatch {
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
        recursive: bool,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        Delivery::new(sink, work)?.start(work, |delivery, work| {
            Self::open_watch(
                directory.directory_handle(),
                delivery,
                false,
                Some(complete),
                recursive,
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
    #[cfg(windows)]
    pub(crate) fn start_directory_with_work<W: SourceDemandReadWork>(
        &self,
        directory: &cap_std::fs::Dir,
        sink: Arc<dyn HostChangeSink>,
        complete: bool,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        use std::os::windows::io::AsHandle;
        let mut delivery = Delivery::new(sink, work)?;
        // The observer funds registration and its retained resources. Physical
        // notifications continue on the root producer's original authority;
        // discovering a directory cannot replace it with a metadata-only leaf.
        delivery.read_authority = Some(self.read_authority());
        delivery.start(work, |delivery, work| {
            Self::open_watch(
                directory.as_handle(),
                delivery,
                false,
                Some(complete),
                false,
                work,
            )
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

    #[cfg(all(test, not(target_os = "linux")))]
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
