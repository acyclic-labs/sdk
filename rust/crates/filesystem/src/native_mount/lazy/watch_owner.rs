use super::*;

/// Owns a watcher until its original physical cleanup completes.
pub struct NativeMountSourceWatchOwner {
    pub(super) watch: Option<Box<dyn SourceWatch>>,
    pub(super) sink: Arc<dyn SourceChangeSink>,
    pub(super) failure: Option<Arc<crate::source_watch::SourceWatchStartupFailure>>,
}

impl NativeMountSourceWatchOwner {
    /// Reports physical watch quiescence.
    pub fn is_stopped(&self) -> bool {
        self.failure.as_ref().map_or_else(
            || self.watch.as_ref().is_none_or(|watch| watch.is_stopped()),
            |failure| failure.is_stopped())
    }

    pub(super) fn stop_with_cleanup_wait(&self, wait: &crate::CancellationToken) -> Result<bool, MountSourceError> {
        self.cleanup(Some(wait))
    }

    pub(super) fn stop_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        self.cleanup(None)
    }

    fn cleanup(&self, wait: Option<&crate::CancellationToken>) -> Result<bool, MountSourceError> {
        if self.is_stopped() { return Ok(false); }
        let result = if let Some(failure) = &self.failure {
            match wait { Some(wait) => failure.stop_with_cleanup_wait(wait), None => failure.stop_accepted_cleanup() }
        } else if let Some(watch) = &self.watch {
            match wait { Some(wait) => watch.stop_with_cleanup_wait(wait), None => watch.stop_accepted_cleanup() }
        } else { return Ok(false); };
        result.map(|()| true).map_err(mount_demand_error)
    }

    pub(super) fn stop(&self, carrier: &mut adapter::NativeDemandCarrier<'_>) -> Result<bool, MountSourceError> {
        if self.is_stopped() { return Ok(false); }
        if let Some(failure) = &self.failure {
            let original = carrier.original_native_work().ok_or(MountSourceError::NativeUnsupported)?;
            return failure.stop_with_work(&mut crate::demand::NativeSourceWork::from_native_materialization_work(&original))
                .map(|()| true).map_err(mount_demand_error);
        }
        let Some(watch) = &self.watch else { return Ok(false); };
        if let Some(original) = carrier.original_native_work() {
            crate::demand::stop_watch(watch.as_ref(), &original, original.cancellation())
                .map(|()| true).map_err(mount_demand_error)
        } else {
            carrier.with_work(|work| {
                let result = crate::demand::stop_watch(watch.as_ref(), work.remaining().map_err(mount_demand_error)?, work.cancellation());
                mount_accept(work, result, mount_demand_error).map(|()| true)
            })
        }
    }
}

impl std::ops::Deref for NativeMountSourceWatchOwner {
    type Target = dyn SourceWatch;
    fn deref(&self) -> &Self::Target {
        self.watch.as_deref().expect("published source retains its watch")
    }
}

impl Drop for NativeMountSourceWatchOwner {
    fn drop(&mut self) {
        drop(self.watch.take());
        drop(self.failure.take());
    }
}
