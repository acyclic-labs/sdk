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
        // The publication index survives replay-cache replacement. Registration
        // and the initial cursor are captured under the same cache lock as writes.
        let (changed, cursor) = self.with_provider(async |memory| {
            let (_, cursor) = memory.prepare_progress(&path, from, anchor).await?;
            let index = self.inner.publications.follow_index(&path);
            let changed = crate::follow_index::FollowIndex::subscribe(&index, filter.clone(), from)?;
            Ok((changed, cursor))
        }).await?;
        Ok(stream::try_unfold(
            (self.clone(), path, cursor, anchor, changed, filter),
            |(provider, path, mut cursor, mut anchor, mut changed, filter)| async move {
                loop {
                    if let Some(record) = provider.with_cached_provider(async |memory| {
                        memory.next_checked_record(&path, &mut cursor, &mut anchor, &filter).await
                    }).await? {
                        return Ok(Some((record, (provider, path, cursor, anchor, changed, filter))));
                    }
                    changed.changed.changed().await.map_err(|_| StreamError::Unavailable)?;
                }
            },
        ).boxed_local())
    }
}
