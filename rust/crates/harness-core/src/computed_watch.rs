//! One retained runtime owner for event-driven Computed subscriptions.
use acyclic_fs::{
    NativeMaterializationWork,
    demand::{DemandError, SourceFence, SourceWatch},
    kernel::{KernelAccounting, RetainedAllocation},
};
use acyclic_native_runtime::{OwnedFuture, retain_future};
use std::future::Future;
struct Watch {
    future: OwnedFuture,
    _allocation: RetainedAllocation,
}

impl SourceWatch for Watch {
    fn fence(&self, ctx: &mut KernelAccounting<'_>) -> Result<SourceFence, DemandError> {
        ctx.remaining()?;
        // Independent input follows do not establish a single delivery cut.
        Ok(SourceFence::Unconfirmed)
    }
    fn stop(&self, ctx: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        ctx.remaining()?;
        self.stop_accepted_cleanup()
    }
    fn stop_with_cleanup_wait(
        &self,
        wait: &acyclic_fs::CancellationToken,
    ) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        self.stop_accepted_cleanup()
    }
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.future.stop().map_err(DemandError::from)
    }
    fn is_stopped(&self) -> bool {
        self.future.is_stopped()
    }
    fn is_exact(&self) -> bool {
        !self.future.is_stopped()
    }
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

pub(crate) fn retain<F: Future<Output = ()> + Send + 'static>(
    make: impl FnOnce() -> F,
    original: &NativeMaterializationWork,
    ctx: &mut KernelAccounting<'_>,
) -> Result<Box<dyn SourceWatch>, DemandError> {
    let allocation = RetainedAllocation::claim(std::mem::size_of::<Watch>() as u64, 1, ctx)?;
    let future = retain_future(make(), &original.runtime_meter())
        .map_err(|error| DemandError::from(original.runtime_error(error)))?;
    Ok(Box::new(Watch {
        future,
        _allocation: allocation,
    }))
}
