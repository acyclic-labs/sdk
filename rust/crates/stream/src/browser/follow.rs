use super::*;

impl BrowserStream {
    pub(super) async fn progress_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        filter: crate::FollowFilter,
    ) -> Result<crate::FollowStream, StreamError> {
        let changed = self
            .inner
            .publications
            .interests
            .subscribe(publication::Key::Records(path.clone()))?;
        let (_, cursor) = self
            .with_provider(async |memory| memory.prepare_progress(&path, from, anchor).await)
            .await?;
        Ok(stream::try_unfold(
            (self.clone(), path, cursor, anchor, changed, filter),
            |(provider, path, mut cursor, mut anchor, mut changed, filter)| async move {
                let update = changed
                    .next(async || {
                        provider
                            .with_cached_provider(async |memory| {
                                memory
                                    .next_follow_update(&path, &mut cursor, &mut anchor, &filter)
                                    .await
                            })
                            .await
                    })
                    .await?;
                Ok(Some((
                    update,
                    (provider, path, cursor, anchor, changed, filter),
                )))
            },
        )
        .boxed_local())
    }
    pub(super) async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        // Register the publication interest before inspecting the cached history.
        // Cache -> memory is the existing lock order; delivery waits hold neither.
        let changed = self
            .inner
            .publications
            .interests
            .subscribe(publication::Key::Records(path.clone()))?;
        let (_, cursor) = self
            .with_provider(async |memory| {
                memory
                    .prepare_follow(&path, from, anchor, filter.clone())
                    .await
            })
            .await?;
        Ok(stream::try_unfold(
            (self.clone(), path, cursor, anchor, changed, filter),
            |(provider, path, mut cursor, mut anchor, mut changed, filter)| async move {
                let record = changed
                    .next(async || {
                        provider
                            .with_cached_provider(async |memory| {
                                memory
                                    .next_checked_record(&path, &mut cursor, &mut anchor, &filter)
                                    .await
                            })
                            .await
                    })
                    .await?;
                Ok(Some((
                    record,
                    (provider, path, cursor, anchor, changed, filter),
                )))
            },
        )
        .boxed_local())
    }
}
