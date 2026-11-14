//! One event-driven owner for the inputs of a subscribed UI view.
use super::*;

use acyclic_stream::{FollowRequest, NativeRecordStream, WhenAbsent};
use futures::{future, stream};

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
    paths: [Option<StreamPath>; 4],
    original: NativeMaterializationWork,
    sink: Arc<dyn ViewChangeSink<Affected>>,
    ctx: &mut KernelAccounting<'_>,
) -> Result<Box<dyn SourceWatch>, DemandError> {
    let errors = original.clone();
    let delivery = Delivery(sink);
    crate::computed_watch::retain(
        || async move {
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
                    let count = paths.iter().flatten().count();
                    let _paths = RetainedAllocation::claim(
                        if count > 1 {
                            (count * std::mem::size_of::<StreamPath>()) as u64
                        } else {
                            0
                        },
                        u64::from(count > 1),
                        &mut ctx,
                    )?;
                    native_result(
                        streams
                            .native_heads(
                                HeadsRequest {
                                    at: None,
                                    at_least: RevisionCut::default(),
                                    paths: paths.iter().flatten().cloned().collect(),
                                    retained: HeadSet::new(),
                                    durable_through: None,
                                    projection: HeadProjection::Tail,
                                    children: None,
                                    limit: paths.iter().flatten().count() as u32,
                                },
                                original.runtime_meter(),
                            )
                            .await,
                        &original,
                    )?
                };
                let selected = paths.each_ref().map(Option::is_some);
                let [conversation, status, children, inbox] = paths;
                let mut conversation = open(&*streams, conversation, &heads, 0, &original).await?;
                let mut status = open(&*streams, status, &heads, 1, &original).await?;
                let mut children = open(&*streams, children, &heads, 2, &original).await?;
                let mut inbox = open(&*streams, inbox, &heads, 3, &original).await?;
                drop(heads);
                // Changes preceding the opening cuts invalidate earlier observations.
                for (input, selected) in [
                    Affected::Conversation,
                    Affected::Status,
                    Affected::Children,
                    Affected::Role,
                ]
                .into_iter()
                .zip(selected)
                {
                    if selected {
                        delivery.0.changed(&input, &mut ctx)?;
                    }
                }
                let mut events = stream::select(
                    events(
                        conversation.as_mut().map(NativeStreamOutput::value_mut),
                        Affected::Conversation,
                        &original,
                    ),
                    stream::select(
                        events(
                            status.as_mut().map(NativeStreamOutput::value_mut),
                            Affected::Status,
                            &original,
                        ),
                        stream::select(
                            events(
                                children.as_mut().map(NativeStreamOutput::value_mut),
                                Affected::Children,
                                &original,
                            ),
                            events(
                                inbox.as_mut().map(NativeStreamOutput::value_mut),
                                Affected::Role,
                                &original,
                            ),
                        ),
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
        &errors,
        ctx,
    )
}

async fn open<P: NativeStreamProvider + ?Sized>(
    streams: &P,
    path: Option<StreamPath>,
    heads: &NativeStreamOutput<Heads>,
    index: usize,
    original: &NativeMaterializationWork,
) -> Result<Option<NativeStreamOutput<NativeRecordStream>>, DemandError> {
    let Some(path) = path else {
        return Ok(None);
    };
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
    .map(Some)
}

fn events<'a>(
    records: Option<&'a mut NativeRecordStream>,
    affected: Affected,
    original: &'a NativeMaterializationWork,
) -> impl futures::Stream<Item = Result<Affected, DemandError>> + Unpin + 'a {
    match records {
        Some(records) => future::Either::Left(
            records
                .map(move |record| native_result(record, original).map(|_| affected))
                .chain(stream::once(future::ready(Err(DemandError::StaleSource)))),
        ),
        None => future::Either::Right(stream::empty()),
    }
}
