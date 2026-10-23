use super::*;
use acyclic_stream::{BoxProviderStream, StreamError};
use futures::{StreamExt, stream};

impl<P: StreamProvider> HistoryReader<P> {
    /// Pushes each verified owner append after a revision without batching or polling.
    pub async fn follow(
        self,
        after_revision: u64,
        maximum_event_bytes: u64,
    ) -> Result<BoxProviderStream<'static, Result<Event>>> {
        if maximum_event_bytes == 0 {
            return Err(Error::Invalid(
                "history follow byte bound must be positive".into(),
            ));
        }
        let source = self
            .client
            .follow(
                self.path.clone(),
                after_revision,
                acyclic_stream::FollowFilter::default(),
            )
            .await
            .map_err(|error| match error {
                StreamError::NotFound => Error::NotFound("aggregate history".into()),
                other => other.into(),
            })?;
        Ok(Box::pin(stream::try_unfold(
            (source, self.verifier, after_revision),
            move |(mut source, verifier, revision)| async move {
                let Some(record) = source.next().await.transpose()? else {
                    return Ok(None);
                };
                if record.value.len() as u64 > maximum_event_bytes {
                    return Err(Error::Invalid(
                        "history record exceeds byte allowance".into(),
                    ));
                }
                let (event, _) = verify_history_record_with_retained_quota(
                    &verifier,
                    verifier.audience(),
                    revision,
                    &record,
                    &mut None,
                )?;
                let revision = event.revision;
                Ok(Some((event, (source, verifier, revision))))
            },
        )))
    }
}
