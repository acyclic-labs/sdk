use super::*;

/// Owns a watcher until physical cleanup completes.
pub struct NativeMountSourceWatchOwner {
    pub(super) watch: Option<Box<dyn SourceWatch>>,
    pub(super) sink: Option<Arc<dyn SourceChangeSink>>,
    pub(super) startup_failure: Option<Arc<crate::source_watch::SourceWatchStartupFailure>>,
    pub(super) bytes: u64,
    pub(super) custody: Option<crate::native_mount::NativeMountAllocationCustody>,
}

impl NativeMountSourceWatchOwner {
    /// Whether this original watcher has confirmed physical native quiescence.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.startup_failure.as_ref().map_or_else(
            || self.watch.as_ref().is_none_or(|watch| watch.is_stopped()),
            |failure| failure.is_stopped(),
        )
    }

    /// Retires this original watch using its before-start cleanup credit.
    pub(in crate::native_mount) fn stop_with_cleanup_wait(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        self.stop_with_cleanup(Some(wait))
    }

    pub(in crate::native_mount) fn stop_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        self.stop_with_cleanup(None)
    }

    pub(super) fn stop_with_cleanup(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, MountSourceError> {
        if self.is_stopped() {
            return Ok(false);
        }
        let result = if let Some(failure) = &self.startup_failure {
            match wait {
                Some(wait) => failure.stop_with_cleanup_wait(wait),
                None => failure.stop_accepted_cleanup(),
            }
        } else if let Some(watch) = &self.watch {
            match wait {
                Some(wait) => watch.stop_with_cleanup_wait(wait),
                None => watch.stop_accepted_cleanup(),
            }
        } else {
            return Ok(false);
        };
        result.map_err(mount_demand_error).map(|()| true)
    }

    /// Stops the original watch on the supplied carrier.
    pub(in crate::native_mount) fn stop(
        &self,
        carrier: &mut adapter::NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        let original = carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeUnsupported)?;
        let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(&original);
        crate::native_host::NativeHostWork::check(&mut work).map_err(mount_demand_error)?;
        if self.is_stopped() {
            return Ok(false);
        }
        let result = if let Some(failure) = &self.startup_failure {
            failure.stop_with_work(&mut work)
        } else if let Some(watch) = &self.watch {
            crate::demand::stop_watch(watch.as_ref(), &original, original.cancellation())
        } else {
            return Ok(false);
        };
        result.map_err(mount_demand_error).map(|()| true)
    }
}

impl Drop for NativeMountSourceWatchOwner {
    fn drop(&mut self) {
        drop(self.watch.take());
        drop(self.sink.take());
        drop(self.startup_failure.take());
        if let Some(custody) = &self.custody {
            custody
                .retire(self.bytes)
                .expect("watch custody retires only after its physical owner and sink drop");
        }
    }
}
