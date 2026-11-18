impl MemoryStream {
    pub(crate) async fn prepare_progress(
        &self,
        path: &StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        window: FollowWindow,
    ) -> Result<PreparedFollow<crate::change::Subscription>, StreamError> {
        self.prepare_cursor(path, from, anchor, when_absent, window, |signals, _| {
            signals.changed.subscribe()
        })
        .await
    }

    pub(crate) async fn next_follow_update(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
        anchor: &mut Option<CommitId>,
        filter: &crate::FollowFilter,
    ) -> Result<Option<crate::FollowUpdate>, StreamError> {
        if !self.refresh_follow(path, cursor, *anchor).await? {
            return Ok(cursor
                .opening_tail
                .take()
                .map(|tail| crate::FollowUpdate::Opening { tail }));
        }
        if let Some(tail) = cursor.opening_tail.take() {
            return Ok(Some(crate::FollowUpdate::Opening { tail }));
        }
        let mut skipped = None;
        for _ in 0..crate::REPLAY_PAGE {
            let Some(record) = cursor.peek_record().await? else {
                break;
            };
            if filter.matches(&record.headers) {
                // Publish the skipped cut first, leaving the matching record in
                // the cursor. Credit cannot reorder it past this observation.
                if skipped.is_some() {
                    break;
                }
                let record = cursor
                    .next_record()
                    .await?
                    .ok_or(StreamError::Unavailable)?;
                if anchor.is_some() {
                    *anchor = Some(record.commit_id);
                }
                return filter
                    .project(record)
                    .map(|record| record.map(crate::FollowUpdate::Record));
            }
            let record = cursor
                .next_record()
                .await?
                .ok_or(StreamError::Unavailable)?;
            skipped = Some(record.commit_id);
            if anchor.is_some() {
                *anchor = skipped;
            }
        }
        Ok(skipped.map(|preceding| crate::FollowUpdate::Progress {
            next: cursor.position(),
            preceding,
        }))
    }

    async fn progress_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        filter: crate::FollowFilter,
        window: FollowWindow,
    ) -> Result<crate::FollowStream, StreamError> {
        let (snapshot, selection, changed, cursor) = self
            .prepare_progress(&path, from, anchor, when_absent, window)
            .await?;
        let anchor = selection.preceding;
        let state = Box::new(MemoryProgress {
            provider: self.clone(),
            path,
            cursor,
            anchor,
            filter,
            changed,
        });
        Ok(crate::Opened::new(
            snapshot,
            selection,
            Box::pin(stream::try_unfold(state, |mut state| async move {
                loop {
                    if state.cursor.finished() {
                        return Ok(None);
                    }
                    #[cfg(loom)]
                    let retain_pending = state.provider.retain_pending_read;
                    let (next, update) = crate::snapshot::read(
                        state,
                        #[cfg(loom)]
                        retain_pending,
                    )
                    .await;
                    state = next;
                    if let Some(update) = update? {
                        return Ok(Some((update, state)));
                    }
                    if state.cursor.finished() {
                        return Ok(None);
                    }
                    state.changed.changed().await.map_err(|_| {
                        if state.anchor.is_some() {
                            StreamError::PrefixNotRetained
                        } else {
                            StreamError::Unavailable
                        }
                    })?;
                }
            })),
        ))
    }
}

struct MemoryProgress {
    provider: MemoryStream,
    path: StreamPath,
    cursor: HistoryCursor,
    anchor: Option<CommitId>,
    filter: crate::FollowFilter,
    changed: crate::change::Subscription,
}
impl crate::snapshot::Snapshot for Box<MemoryProgress> {
    type Output = Option<crate::FollowUpdate>;
    async fn read(&mut self) -> Result<Self::Output, StreamError> {
        let state = &mut **self;
        state
            .provider
            .next_follow_update(
                &state.path,
                &mut state.cursor,
                &mut state.anchor,
                &state.filter,
            )
            .await
    }
}
