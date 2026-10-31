impl MemoryStream {
    pub(crate) async fn prepare_progress(
        &self,
        path: &StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        bootstrap_head: bool,
        until: Option<u64>,
    ) -> Result<(crate::Revision, crate::StreamSelection, watch::Receiver<u64>, HistoryCursor), StreamError> {
        let state = self.state.write().await;
        let snapshot = state.paths.get(path).cloned();
        let revision = self.revisions.at(state.decision);
        let selection = follow_selection(snapshot.as_ref(), from, anchor, bootstrap_head, until)?;
        let (from, anchor) = (selection.from, selection.preceding);
        let tail = snapshot.as_ref().map_or(0, |path| path.tail);
        let registration = state
            .signals
            .subscribe_with(path.clone(), || Signals::new(tail))?;
        let signals = registration.value();
        let changed = signals.changed.subscribe();
        drop(state);
        let mut cursor =
            HistoryCursor::initial(snapshot.as_ref(), from, anchor, when_absent).await?;
        cursor.registration = Some(registration);
        // Capture under the same state lock as interest registration. Even an
        // absent waiting stream has an empty opening cut, without a tail probe.
        cursor.opening_tail = Some(tail);
        cursor.end = selection.end;
        Ok((revision, selection, changed, cursor))
    }

    pub(crate) async fn next_follow_update(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
        anchor: &mut Option<CommitId>,
        filter: &crate::FollowFilter,
    ) -> Result<Option<crate::FollowUpdate>, StreamError> {
        if cursor.end.is_none() {
        let state = self.state.read().await;
        let snapshot = state.paths.get(path).cloned();
        drop(state);
        let path = snapshot.as_ref();
        if !cursor.observe_creation(path, *anchor).await? {
            return Ok(cursor
                .opening_tail
                .take()
                .map(|tail| crate::FollowUpdate::Opening { tail }));
        }
        let path = path.ok_or(if anchor.is_some() {
            StreamError::PrefixNotRetained
        } else {
            StreamError::NotFound
        })?;
        if cursor.position() > path.tail {
            return Err(StreamError::OutOfRange);
        }
        if let Some(anchor) = anchor {
            cursor.check_anchor(path, *anchor).await?;
        }
        cursor.sync(path);
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
        bootstrap_head: bool,
        until: Option<u64>,
    ) -> Result<crate::FollowStream, StreamError> {
        let (snapshot, selection, changed, cursor) = self
            .prepare_progress(&path, from, anchor, when_absent, bootstrap_head, until)
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
        Ok(crate::Opened::new(snapshot, selection, stream::try_unfold(state, |mut state| async move {
            loop {
                if state.cursor.finished() { return Ok(None); }
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
                if state.cursor.finished() { return Ok(None); }
                state.changed.changed().await.map_err(|_| {
                    if state.anchor.is_some() {
                        StreamError::PrefixNotRetained
                    } else {
                        StreamError::Unavailable
                    }
                })?;
            }
        })
        .boxed()))
    }
}

struct MemoryProgress {
    provider: MemoryStream,
    path: StreamPath,
    cursor: HistoryCursor,
    anchor: Option<CommitId>,
    filter: crate::FollowFilter,
    changed: watch::Receiver<u64>,
}
impl crate::snapshot::Snapshot for Box<MemoryProgress> {
    type Output = Result<Option<crate::FollowUpdate>, StreamError>;
    async fn read(&mut self) -> Self::Output {
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
