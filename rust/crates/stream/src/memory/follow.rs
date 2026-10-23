impl MemoryStream {
    pub(crate) async fn prepare_follow(
        &self,
        path: &StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        filter: crate::FollowFilter,
    ) -> Result<(crate::follow_index::FollowInterest, HistoryCursor), StreamError> {
        let mut state = self.state.write().await;
        let path = state.paths.get_mut(path).ok_or(StreamError::NotFound)?;
        let cursor = HistoryCursor::prepare(path, from, anchor)?;
        let index = path.follows.get_or_insert_with(|| {
            Arc::new(crate::query_sync::SyncMutex::new(
                crate::follow_index::FollowIndex::default(),
            ))
        });
        Ok((
            crate::follow_index::FollowIndex::subscribe(index, filter, path.tail)?,
            cursor,
        ))
    }

    pub(crate) async fn next_checked_record(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
        anchor: &mut Option<CommitId>,
        filter: &crate::FollowFilter,
    ) -> Result<Option<Record>, StreamError> {
        let state = self.state.read().await;
        let path = state.paths.get(path).ok_or(StreamError::NotFound)?;
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
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        let (interest, cursor) = self
            .prepare_follow(&path, from, anchor, filter.clone())
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
                #[cfg(acyclic_loom)]
                let retain_pending = state.provider.retain_pending_read;
                let (next, record) = crate::snapshot::read(
                    state,
                    #[cfg(acyclic_loom)]
                    retain_pending,
                )
                .await;
                state = next;
                match record {
                    Ok(Some(record)) => break Ok(record),
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
