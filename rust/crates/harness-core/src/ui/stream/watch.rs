//! One event-driven owner for the three inputs of a subscribed UI view.
use super::*;
use acyclic_native_runtime::{OwnedFuture, retain_future};
use acyclic_stream::{FollowRequest, NativeRecordStream, WhenAbsent};
use futures::{future, stream};

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

// Retiring the frame invalidates delivery on every exit, including cancellation
// while an opening floor or snapshot lock is pending. No cleanup task escapes.
struct Delivery(Arc<dyn ViewChangeSink<Affected>>);
impl Drop for Delivery {
    fn drop(&mut self) {
        self.0.delivery_lost();
    }
}

pub(super) fn start<P: NativeStreamProvider + ?Sized + 'static>(
    streams: Arc<P>,
    paths: [StreamPath; 3],
    original: NativeMaterializationWork,
    sink: Arc<dyn ViewChangeSink<Affected>>,
    ctx: &mut KernelAccounting<'_>,
) -> Result<Box<dyn SourceWatch>, DemandError> {
    let allocation = RetainedAllocation::claim(std::mem::size_of::<Watch>() as u64, 1, ctx)?;
    let meter = original.runtime_meter();
    let errors = original.clone();
    let delivery = Delivery(sink);
    let future = retain_future(
        async move {
            // Register the original cancellation event before any opening wait.
            let Ok(cancelled) = original.cancellation_wait() else {
                return;
            };
            let run = async {
                let cancellation = original.cancellation();
                let mut ctx = KernelAccounting::NativeOriginal {
                    work: &original,
                    cancellation: &cancellation,
                };
                // One starting cut avoids replaying history. The existing
                // multi-path request captures all inputs without a cache.
                let heads = {
                    let _paths = RetainedAllocation::claim(
                        std::mem::size_of_val(&paths) as u64,
                        1,
                        &mut ctx,
                    )?;
                    native_result(
                        streams
                            .native_heads(
                                HeadsRequest {
                                    at: None,
                                    at_least: RevisionCut::default(),
                                    paths: paths.iter().cloned().collect(),
                                    retained: HeadSet::new(),
                                    durable_through: None,
                                    projection: HeadProjection::Tail,
                                    children: None,
                                    limit: paths.len() as u32,
                                },
                                original.runtime_meter(),
                            )
                            .await,
                        &original,
                    )?
                };
                let [conversation, status, children] = paths;
                let mut conversation = open(&*streams, conversation, &heads, 0, &original).await?;
                let mut status = open(&*streams, status, &heads, 1, &original).await?;
                let mut children = open(&*streams, children, &heads, 2, &original).await?;
                drop(heads);
                // Changes preceding the opening cuts invalidate earlier observations.
                for input in [Affected::Conversation, Affected::Status, Affected::Children] {
                    delivery.0.changed(&input, &mut ctx)?;
                }
                let mut events = stream::select(
                    events(conversation.value_mut(), Affected::Conversation, &original),
                    stream::select(
                        events(status.value_mut(), Affected::Status, &original),
                        events(children.value_mut(), Affected::Children, &original),
                    ),
                );
                while let Some(input) = events.next().await {
                    delivery.0.changed(&input?, &mut ctx)?;
                }
                Ok::<(), DemandError>(())
            };
            let _ = future::select(std::pin::pin!(cancelled), std::pin::pin!(run)).await;
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

async fn open<P: NativeStreamProvider + ?Sized>(
    streams: &P,
    path: StreamPath,
    heads: &NativeStreamOutput<Heads>,
    index: usize,
    original: &NativeMaterializationWork,
) -> Result<NativeStreamOutput<NativeRecordStream>, DemandError> {
    let from = heads
        .value()
        .heads
        .get(index)
        .and_then(Option::as_ref)
        .map_or(0, |head| head.tail);
    let cancellation = original.cancellation();
    let mut ctx = KernelAccounting::NativeOriginal {
        work: original,
        cancellation: &cancellation,
    };
    let _floor = RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, &mut ctx)?;
    let mut request = FollowRequest::new(path, from);
    request.when_absent = WhenAbsent::Wait;
    request.at_least = RevisionCut::of(heads.value().snapshot.clone());
    native_result(
        streams
            .native_follow(request, original.runtime_meter())
            .await,
        original,
    )
}

fn events<'a>(
    records: &'a mut NativeRecordStream,
    affected: Affected,
    original: &'a NativeMaterializationWork,
) -> impl futures::Stream<Item = Result<Affected, DemandError>> + Unpin + 'a {
    records
        .map(move |record| native_result(record, original).map(|_| affected))
        .chain(stream::once(future::ready(Err(DemandError::StaleSource))))
}
