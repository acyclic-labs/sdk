use super::*;

impl BrowserStream {
    pub(super) async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
    ) -> Result<RecordStream, StreamError> {
        // Register the publication interest before inspecting the cached history.
        // Cache -> memory is the existing lock order; delivery waits hold neither.
        let changed = self
            .inner
            .publications
            .interests
            .subscribe(publication::Key::Records(path.clone()))?;
        let (_, cursor) = self
            .with_provider(async |memory| memory.prepare_follow(&path, from, anchor).await)
            .await?;
        Ok(stream::try_unfold(
            (self.clone(), path, cursor, anchor, changed),
            |(provider, path, mut cursor, mut anchor, mut changed)| async move {
                let record = changed
                    .next(async || {
                        provider
                            .with_cached_provider(async |memory| {
                                memory
                                    .next_checked_record(&path, &mut cursor, &mut anchor)
                                    .await
                            })
                            .await
                    })
                    .await?;
                Ok(Some((record, (provider, path, cursor, anchor, changed))))
            },
        )
        .boxed_local())
    }
}
