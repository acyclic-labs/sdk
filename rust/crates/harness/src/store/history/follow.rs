use super::*;
use acyclic_stream::{BoxProviderStream, StreamError};
use futures::{FutureExt, StreamExt, stream};

impl<P: StreamProvider> HistoryReader<P> {
    /// Follows verified history after a revision, batching only immediately ready records.
    /// Each batch obeys both bounds; an idle stream waits for the next owner append.
    pub async fn follow(
        self,
        after_revision: u64,
        limits: HistoryReadLimits,
    ) -> Result<BoxProviderStream<'static, Result<Vec<Event>>>> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "history follow bounds must be positive".into(),
            ));
        }
        let source = self
            .client
            .follow(self.path.clone(), after_revision)
            .await
            .map_err(|error| match error {
                StreamError::NotFound => Error::NotFound("aggregate history".into()),
                other => other.into(),
            })?
            .fuse();
        Ok(Box::pin(stream::try_unfold(
            (source, self.verifier, after_revision, None),
            move |(mut source, verifier, mut revision, pending)| async move {
                let mut next = match pending {
                    Some(record) => Some(Ok(record)),
                    None => source.next().await,
                };
                let mut events = Vec::new();
                let mut bytes = 0;
                let mut pending = None;
                while let Some(record) = next {
                    let record = record?;
                    let length = record.value.len() as u64;
                    if length > limits.maximum_bytes - bytes {
                        if events.is_empty() {
                            return Err(Error::Invalid(
                                "history record exceeds byte allowance".into(),
                            ));
                        }
                        pending = Some(record);
                        break;
                    }
                    let (event, _) = verify_history_record_with_retained_quota(
                        &verifier,
                        verifier.audience(),
                        revision,
                        &record,
                        &mut None,
                    )?;
                    revision = event.revision;
                    bytes += length;
                    events.push(event);
                    if events.len() == limits.maximum_events as usize {
                        break;
                    }
                    next = source.next().now_or_never().flatten();
                }
                Ok((!events.is_empty()).then_some((events, (source, verifier, revision, pending))))
            },
        )))
    }
}
