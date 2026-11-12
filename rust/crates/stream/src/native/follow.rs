use super::*;
use acyclic_native_runtime::ReadAllocation;

pub(crate) fn validate_follow(request: &FollowRequest) -> Result<(), StreamError> {
    if request.progress
        || request.bootstrap_head
        || request.until.is_some()
        || request.preceding.is_some()
        || request.filter != FollowFilter::default()
    {
        return Err(StreamError::Unsupported);
    }
    Ok(())
}

pin_project_lite::pin_project! {
    struct FundedStream<S> {
        #[pin]
        stream: S,
        _allocation: ReadAllocation,
    }
}
impl<S: futures::Stream> futures::Stream for FundedStream<S> {
    type Item = S::Item;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(context)
    }
}

/// The exact boxed stream owns its allocation through its physical destructor.
/// Provider-global registration indexes remain with the provider; no copied
/// caller budget or payload owner is introduced by this adapter.
pub(crate) fn boxed_stream<S>(
    stream: S,
    work: Option<&Arc<Work>>,
) -> Result<crate::BoxProviderStream<'static, S::Item>, StreamError>
where
    S: futures::Stream + Send + 'static,
{
    match work {
        Some(work) => {
            let allocation = work.record(ReadAllocation::new(
                &work.meter,
                std::mem::size_of::<FundedStream<S>>(),
            ))?;
            Ok(Box::pin(FundedStream {
                stream,
                _allocation: allocation,
            }))
        }
        None => Ok(Box::pin(stream)),
    }
}

pub(crate) fn follow_records(
    follow: FollowStream,
    work: &Arc<Work>,
) -> Result<NativeRecordStream, StreamError> {
    use futures::StreamExt as _;
    let Opened {
        snapshot,
        selection,
        cursor,
    } = follow;
    let original = Arc::clone(work);
    let records = cursor.map(move |record| {
        original.finish(record.and_then(|update| match update {
            FollowUpdate::Record(record) => Ok((record, None)),
            _ => Err(StreamError::InvalidResponse),
        }))
    });
    Ok(Opened::new(
        snapshot,
        selection,
        boxed_stream(records, Some(work))?,
    ))
}
