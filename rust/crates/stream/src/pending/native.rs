use super::*;
use crate::{NativeProviderFuture, NativeStreamOutput, NativeStreamResult};
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation, arc_allocation_bytes};

/// The same pending completion, retaining the original native result and ledger.
pub type NativePending<H = ReadAllocation> =
    Pending<NativeStreamResult<CommitOutcome>, (H, ReadAllocation)>;

impl<H: Send + Sync + 'static> NativePending<H> {
    /// Prepares one native producer for the caller's original execution owner.
    ///
    /// The returned future is the sole producer. Drive or transfer it once;
    /// dropping it ends every observer with `Unavailable`, even before first
    /// poll. Cancelling an observer does not drop this independently owned
    /// future. No portable provider method, new work root or retry is used.
    /// Control vectors, completion and concrete producer frame are admitted
    /// before allocation. Payloads and headers retain their existing owners.
    /// `storage` is the original request's already-owned custody, transferred
    /// intact; it is not released and re-admitted through another ledger.
    pub fn prepare_native<P: crate::NativeStreamProvider + ?Sized>(
        provider: Arc<P>,
        request: CommitRequest,
        storage: H,
        meter: Arc<dyn NativeReadMeter>,
    ) -> std::io::Result<(Self, NativeProviderFuture<'static, ()>)> {
        let copied = request.try_clone_native(&meter)?;
        let completion = ReadAllocation::new(
            &meter,
            arc_allocation_bytes::<
                Completion<NativeStreamResult<CommitOutcome>, (H, ReadAllocation)>,
            >(),
        )?;
        let (pending, producer) = Self::prepare(request, (storage, completion));
        let future = NativeProviderFuture::try_build(&meter, || {
            let meter = Arc::clone(&meter);
            async move {
                let (command, allocation) = copied.into_parts();
                let result = provider
                    .native_commit(command, None, Arc::clone(&meter))
                    .await;
                drop(allocation);
                producer.publish(result);
                Ok(Ok(NativeStreamOutput::new(
                    (),
                    ReadAllocation::empty(&meter),
                )))
            }
        })?;
        Ok((pending, future))
    }
}
