//! A subscribed evaluation directory owns one original conversation follow.
use super::*;
use acyclic_native_runtime::{OwnedFuture, retain_future};
use acyclic_stream::{FollowRequest, WhenAbsent};

struct Watch {
    future: OwnedFuture,
    _allocation: RetainedAllocation,
}
impl SourceWatch for Watch {
    fn fence(&self, ctx: &mut KernelAccounting<'_>) -> Result<SourceFence, DemandError> {
        ctx.remaining()?;
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
struct Delivery(Arc<dyn ViewChangeSink<()>>);
impl Drop for Delivery {
    fn drop(&mut self) {
        self.0.delivery_lost();
    }
}

pub(super) fn start<P: NativeStreamProvider + ?Sized + 'static>(
    streams: Arc<P>,
    path: StreamPath,
    original: NativeMaterializationWork,
    sink: Arc<dyn ViewChangeSink<()>>,
    ctx: &mut KernelAccounting<'_>,
) -> Result<Box<dyn SourceWatch>, DemandError> {
    let allocation = RetainedAllocation::claim(std::mem::size_of::<Watch>() as u64, 1, ctx)?;
    let meter = original.runtime_meter();
    let errors = original.clone();
    let delivery = Delivery(sink);
    let future = retain_future(
        async move {
            let Ok(cancelled) = original.cancellation_wait() else {
                return;
            };
            let run = async {
                let cancellation = original.cancellation();
                let mut ctx = KernelAccounting::NativeOriginal {
                    work: &original,
                    cancellation: &cancellation,
                };
                let heads = result(
                    streams
                        .native_heads(
                            HeadsRequest {
                                at: None,
                                at_least: RevisionCut::default(),
                                paths: HeadSet::from_buf([super::path(&path, &original)?]),
                                retained: HeadSet::new(),
                                durable_through: None,
                                projection: HeadProjection::Tail,
                                children: None,
                                limit: 1,
                            },
                            original.runtime_meter(),
                        )
                        .await,
                    &original,
                )?;
                let tail = heads
                    .value()
                    .heads
                    .first()
                    .and_then(Option::as_ref)
                    .map_or(0, |head| head.tail);
                let _floor =
                    RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, &mut ctx)?;
                let mut request = FollowRequest::new(path, tail);
                request.when_absent = WhenAbsent::Wait;
                request.at_least = RevisionCut::of(heads.value().snapshot.clone());
                let mut records = result(
                    streams
                        .native_follow(request, original.runtime_meter())
                        .await,
                    &original,
                )?;
                drop(heads);
                // Establish the subscription before invalidating observations made
                // at an earlier opening cut. No timer or readiness probe is used.
                delivery.0.changed(&(), &mut ctx)?;
                while let Some(record) = records.value_mut().next().await {
                    let _record = result(record, &original)?;
                    delivery.0.changed(&(), &mut ctx)?;
                }
                Err::<(), DemandError>(DemandError::StaleSource)
            };
            let _ = futures::future::select(std::pin::pin!(cancelled), std::pin::pin!(run)).await;
            drop(delivery);
        },
        &meter,
    )
    .map_err(|error| DemandError::from(errors.runtime_error(error)))?;
    Ok(Box::new(Watch {
        future,
        _allocation: allocation,
    }))
}
