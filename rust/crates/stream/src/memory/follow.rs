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
        let provider = self.clone();
        Ok(stream::unfold(
            (provider, path, receiver, cursor, anchor, false),
            |(provider, path, mut receiver, mut cursor, mut anchor, done)| async move {
                if done {
                    return None;
                }
                let result = loop {
                    match provider
                        .next_checked_record(&path, &mut cursor, &mut anchor)
                        .await
                    {
                        Ok(Some(record)) => break Ok(record),
                        Err(error) => break Err(error),
                        Ok(None) => {
                            if receiver.changed().await.is_err() {
                                break Err(if anchor.is_some() {
                                    StreamError::PrefixNotRetained
                                } else {
                                    StreamError::Unavailable
                                });
                            }
                        }
                    }
                };
                let done = result.is_err();
                Some((result, (provider, path, receiver, cursor, anchor, done)))
            },
        )
        .boxed())
    }
}
