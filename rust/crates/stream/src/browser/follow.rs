use super::*;

impl BrowserStream {
    pub(super) async fn progress_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        filter: crate::FollowFilter,
        bootstrap_head: bool,
        until: Option<u64>,
    ) -> Result<crate::FollowStream, StreamError> {
        let changed = self
            .inner
            .publications
            .interests
            .subscribe(publication::Key::Records(path.clone()))?;
        let (snapshot, selection, _, cursor) = self
            .with_provider(async |memory| {
                memory
                    .prepare_progress(&path, from, anchor, when_absent, bootstrap_head, until)
                    .await
            })
            .await?;
        let anchor = selection.preceding;
        Ok(crate::Opened::new(
            snapshot,
            selection,
            stream::try_unfold(
                (self.clone(), path, cursor, anchor, changed, filter),
                |(provider, path, mut cursor, mut anchor, mut changed, filter)| async move {
                    let update = changed
                        .next(async || {
                            if cursor.finished() {
                                return Ok(Some(None));
                            }
                            let update = provider
                                .with_cached_provider(async |memory| {
                                    memory
                                        .next_follow_update(
                                            &path,
                                            &mut cursor,
                                            &mut anchor,
                                            &filter,
                                        )
                                        .await
                                })
                                .await?;
                            Ok(if update.is_some() || cursor.finished() {
                                Some(update)
                            } else {
                                None
                            })
                        })
                        .await?;
                    let Some(update) = update else {
                        return Ok(None);
                    };
                    Ok(Some((
                        update,
                        (provider, path, cursor, anchor, changed, filter),
                    )))
                },
            )
            .boxed_local(),
        ))
    }
    pub(super) async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        filter: crate::FollowFilter,
        bootstrap_head: bool,
        until: Option<u64>,
    ) -> Result<crate::FollowStream, StreamError> {
        // The publication index survives replay-cache replacement. Registration
        // and the initial cursor are captured under the same cache lock as writes.
        let (snapshot, selection, changed, cursor, registration) = self
            .with_provider(async |memory| {
                let (snapshot, selection, _, cursor) = memory
                    .prepare_progress(&path, from, anchor, when_absent, bootstrap_head, until)
                    .await?;
                let registration = self.inner.publications.follow_index(&path)?;
                let index = registration.value();
                let changed =
                    crate::follow_index::FollowIndex::subscribe(index, filter.clone(), from)?;
                Ok((snapshot, selection, changed, cursor, registration))
            })
            .await?;
        let anchor = selection.preceding;
        Ok(
            crate::Opened::new(
                snapshot,
                selection,
                stream::try_unfold(
                    (
                        self.clone(),
                        path,
                        cursor,
                        anchor,
                        changed,
                        filter,
                        registration,
                    ),
                    |(
                        provider,
                        path,
                        mut cursor,
                        mut anchor,
                        mut changed,
                        filter,
                        registration,
                    )| async move {
                        loop {
                            if cursor.finished() {
                                return Ok(None);
                            }
                            if let Some(record) = provider
                                .with_cached_provider(async |memory| {
                                    memory
                                        .next_checked_record(
                                            &path,
                                            &mut cursor,
                                            &mut anchor,
                                            &filter,
                                        )
                                        .await
                                })
                                .await?
                            {
                                return Ok(Some((
                                    crate::FollowUpdate::Record(record),
                                    (
                                        provider,
                                        path,
                                        cursor,
                                        anchor,
                                        changed,
                                        filter,
                                        registration,
                                    ),
                                )));
                            }
                            if cursor.finished() {
                                return Ok(None);
                            }
                            changed
                                .changed
                                .changed()
                                .await
                                .map_err(|_| StreamError::unavailable())?;
                        }
                    },
                )
                .boxed_local(),
            ),
        )
    }
}
