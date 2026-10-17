use super::*;
use acyclic_stream::BoxProviderStream;
use futures::stream;

impl<P: StreamProvider> DistributedCoordinator<P> {
    /// Snapshots an operation and follows scheduler changes without a replay/subscription gap.
    pub async fn operation_changes(
        self,
        operation: OperationId,
    ) -> Result<BoxProviderStream<'static, Result<wire::OperationStatus>>> {
        let initial = crate::wire_api::operation_status(
            self.scheduler
                .operation(operation)
                .ok_or_else(|| Error::NotFound("operation".into()))?,
        );
        let revision = initial.revision;
        let source = Some(operations::ObservedRecords::Ordinary(
            self.stream.0.follow(self.revision).await?,
        ));
        Ok(Box::pin(stream::try_unfold(
            (self, source, Some(initial), revision),
            move |(mut coordinator, mut source, initial, mut revision)| async move {
                if let Some(status) = initial {
                    return Ok(Some((status, (coordinator, source, None, revision))));
                }
                loop {
                    let mut work = None;
                    let mut quota = CodecQuota::new(&mut work, 0)?;
                    let mut next = coordinator.revision;
                    let Some(record) = next_record(
                        &mut source,
                        &mut next,
                        next_revision(coordinator.revision)?,
                        &mut quota,
                    )
                    .await?
                    else {
                        return Ok(None);
                    };
                    coordinator.apply_record(record, &mut quota).await?;
                    let state = coordinator
                        .scheduler
                        .operation(operation)
                        .ok_or_else(|| Error::NotFound("operation".into()))?;
                    if state.revision != revision {
                        revision = state.revision;
                        let status = crate::wire_api::operation_status(state);
                        return Ok(Some((status, (coordinator, source, None, revision))));
                    }
                }
            },
        )))
    }
}
