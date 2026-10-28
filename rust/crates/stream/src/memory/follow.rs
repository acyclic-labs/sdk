impl MemoryStream {
    pub(crate) async fn prepare_follow(
        &self,
        path: &StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        filter: crate::FollowFilter,
    ) -> Result<(crate::follow_index::FollowInterest, HistoryCursor), StreamError> {
        let state = self.state.write().await;
        let stream = state.paths.get(path);
        let mut cursor = HistoryCursor::initial(stream, from, anchor, when_absent)?;
        let tail = stream.map_or(0, |path| path.tail);
        let registration = state
            .signals
            .subscribe_with(path.clone(), || Signals::new(tail))?;
        let signals = registration.value();
        let interest = crate::follow_index::FollowIndex::subscribe(&signals.follows, filter, tail)?;
        cursor.registration = Some(registration);
        Ok((interest, cursor))
    }

    pub(crate) async fn next_checked_record(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
        anchor: &mut Option<CommitId>,
        filter: &crate::FollowFilter,
    ) -> Result<Option<Record>, StreamError> {
        let state = self.state.read().await;
        let path = state.paths.get(path);
        if !cursor.observe_creation(path, *anchor)? {
            return Ok(None);
        }
        let path = path.ok_or(if anchor.is_some() {
            StreamError::PrefixNotRetained
        } else {
            StreamError::NotFound
        })?;
        if cursor.next > path.tail {
            return Err(StreamError::OutOfRange);
        }
        if let Some(anchor) = anchor {
            cursor.check_anchor(path, *anchor)?;
        }
        cursor.sync(path.history.as_ref(), path.tail);
        while let Some(record) = cursor.next_record() {
            if anchor.is_some() {
                *anchor = Some(record.commit_id);
            }
            if let Some(record) = filter.project(record)? {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        filter: crate::FollowFilter,
    ) -> Result<crate::FollowStream, StreamError> {
        let (interest, cursor) = self
            .prepare_follow(&path, from, anchor, when_absent, filter.clone())
            .await?;
        let state = Box::new(MemoryFollow {
            provider: self.clone(),
            path,
            interest,
            filter,
            cursor,
            anchor,
            done: false,
        });
        Ok(stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            let result = loop {
                #[cfg(loom)]
                let retain_pending = state.provider.retain_pending_read;
                let (next, record) = crate::snapshot::read(
                    state,
                    #[cfg(loom)]
                    retain_pending,
                )
                .await;
                state = next;
                match record {
                    Ok(Some(record)) => break Ok(crate::FollowUpdate::Record(record)),
                    Err(error) => break Err(error),
                    Ok(None) => {
                        if state.interest.changed.changed().await.is_err() {
                            break Err(if state.anchor.is_some() {
                                StreamError::PrefixNotRetained
                            } else {
                                StreamError::Unavailable
                            });
                        }
                    }
                }
            };
            state.done = result.is_err();
            Some((result, state))
        })
        .boxed())
    }
}

struct MemoryFollow {
    provider: MemoryStream,
    path: StreamPath,
    interest: crate::follow_index::FollowInterest,
    filter: crate::FollowFilter,
    cursor: HistoryCursor,
    anchor: Option<CommitId>,
    done: bool,
}
impl crate::snapshot::Snapshot for Box<MemoryFollow> {
    type Output = Result<Option<Record>, StreamError>;
    async fn read(&mut self) -> Self::Output {
        let state = &mut **self;
        state
            .provider
            .next_checked_record(
                &state.path,
                &mut state.cursor,
                &mut state.anchor,
                &state.filter,
            )
            .await
    }
}
