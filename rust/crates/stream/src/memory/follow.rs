impl MemoryStream {
    pub(crate) async fn prepare_follow(
        &self,
        path: &StreamPath,
        from: u64,
        anchor: Option<CommitId>,
    ) -> Result<(watch::Receiver<u64>, HistoryCursor), StreamError> {
        let state = self.state.read().await;
        let path = state.paths.get(path).ok_or(StreamError::NotFound)?;
        if from > path.tail {
            return Err(StreamError::OutOfRange);
        }
        let mut cursor = HistoryCursor::new(
            path,
            path.tail,
            if anchor.is_some() {
                from.checked_sub(1).ok_or(StreamError::InvalidArgument)?
            } else {
                from
            },
        );
        if let Some(anchor) = anchor {
            let record = cursor.next_record().ok_or(StreamError::PrefixNotRetained)?;
            if record.sequence != from - 1 || record.commit_id != anchor {
                return Err(StreamError::PrefixNotRetained);
            }
        }
        Ok((path.changed.subscribe(), cursor))
    }

    pub(crate) async fn next_checked_record(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
        anchor: &mut Option<CommitId>,
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
        let record = cursor.next_record();
        if anchor.is_some()
            && let Some(record) = &record
        {
            *anchor = Some(record.commit_id);
        }
        Ok(record)
    }

    async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<CommitId>,
    ) -> Result<RecordStream, StreamError> {
        let (receiver, cursor) = self.prepare_follow(&path, from, anchor).await?;
        let state = Box::new(MemoryFollow {
            provider: self.clone(),
            path,
            changed: receiver,
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
                        if state.changed.changed().await.is_err() {
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
    changed: watch::Receiver<u64>,
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
            .next_checked_record(&state.path, &mut state.cursor, &mut state.anchor)
            .await
    }
}
