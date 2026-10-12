use super::*;

/// Owns a watcher until physical cleanup completes.
/// Physical custody of the original source watcher and its constructor sink.
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
        &self, wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        self.stop_with_cleanup(Some(wait))
    }

    pub(in crate::native_mount) fn stop_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        self.stop_with_cleanup(None)
    }

    pub(super) fn stop_with_cleanup(
        &self, wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, MountSourceError> {
        if self.is_stopped() { return Ok(false); }
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
        &self, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        if self.custody.is_some()
            && self.watch.is_some()
            && !matches!(carrier, crate::native_mount::adapter::NativeDemandCarrier::Native(work)
                if work.resource_allowance().is_some()) {
            return self.stop_ordinary_with_carrier(carrier);
        }
        match carrier {
            crate::native_mount::adapter::NativeDemandCarrier::Borrowed(work) => {
                let loan = crate::native_mount::adapter::BorrowedNativeDemandMeter::new(work)?;
                loan.run(|original| self.stop_on_native_work(crate::demand::NativeSourceWork::new(original)))
            }
            crate::native_mount::adapter::NativeDemandCarrier::Shared(original) => self.stop_on_native_work(crate::demand::NativeSourceWork::new(original)),
            crate::native_mount::adapter::NativeDemandCarrier::Native(work) => {
                let original = work.clone();
                self.stop_on_native_work(
                    crate::demand::NativeSourceWork::from_native_materialization_work(&original))
            }
        }
    }

    fn stop_ordinary_with_carrier(
        &self, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        let (budget, cancellation, prior_live) = carrier.with_work(|work| {
            let budget = work.remaining().map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>((budget, work.cancellation().clone(), work.live_allocation_bytes()))
        })?;
        if self.is_stopped() { return Ok(false); }
        let watch = self.watch.as_ref().expect("ordinary construction retains its original watch");
        let result = crate::demand::stop_watch(watch.as_ref(), budget, &cancellation);

        match result {
            Ok(receipt) => carrier.with_work(|work|
                work.merge_with_prior_live(receipt.work, prior_live))
                .map_err(mount_demand_error).map(|()| true),
            Err(failure) => {
                let _ = carrier.with_work(|work|
                    work.merge_with_prior_live(*failure.work, prior_live));
                Err(mount_demand_error(failure.error))
            }
        }
    }

    fn stop_on_native_work(
        &self, mut work: crate::demand::NativeSourceWork<'_>,
    ) -> Result<bool, MountSourceError> {
        crate::native_host::NativeHostWork::check(&mut work).map_err(mount_demand_error)?;
        if self.is_stopped() { return Ok(false); }
        let result = if let Some(failure) = &self.startup_failure {
            failure.stop_with_work(&mut work)
        } else if let Some(watch) = &self.watch {
            let original = work.materialization_work().map_err(mount_demand_error)?;
            return crate::demand::stop_watch(watch.as_ref(), &original, original.cancellation()).map_err(mount_demand_error).map(|()| true);
        } else {
            return Ok(false);
        };
        result.map_err(mount_demand_error).map(|()| true)
    }


}

impl std::ops::Deref for NativeMountSourceWatchOwner {
    type Target = dyn SourceWatch;
    fn deref(&self) -> &Self::Target {
        self.watch.as_deref().expect("published source watch retains its original owner")
    }
}

impl Drop for NativeMountSourceWatchOwner {
    fn drop(&mut self) {
        drop(self.watch.take());
        drop(self.sink.take());
        drop(self.startup_failure.take());
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes)
                .expect("watch custody retires only after its physical owner and sink drop");
        }
    }
}
