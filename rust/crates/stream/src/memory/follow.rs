fn follow_selection(
    snapshot: Option<&PathState>,
    from: u64,
    preceding: Option<CommitId>,
    bootstrap_head: bool,
    until: Option<u64>,
) -> Result<crate::StreamSelection, StreamError> {
    if bootstrap_head && (from != 0 || preceding.is_some() || until.is_some()) {
        return Err(StreamError::InvalidArgument);
    }
    if until.is_some() && snapshot.is_none() {
        return Err(StreamError::NotFound);
    }
    let bootstrap = bootstrap_head
        .then(|| snapshot.and_then(last_record))
        .flatten();
    Ok(crate::StreamSelection {
        from: if bootstrap_head {
            snapshot.map_or(from, |path| path.tail)
        } else {
            from
        },
        preceding: if bootstrap_head {
            bootstrap.as_ref().map(|record| record.commit_id)
        } else {
            preceding
        },
        end: until.map(|end| end.min(snapshot.map_or(0, |path| path.tail))),
        bootstrap,
    })
}

impl MemoryStream {
    pub(crate) async fn prepare_follow(
        &self,
        path: &StreamPath,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
        filter: crate::FollowFilter,
        bootstrap_head: bool,
        until: Option<u64>,
    ) -> Result<
        (
            crate::Revision,
            crate::StreamSelection,
            crate::follow_index::FollowInterest,
            HistoryCursor,
        ),
        StreamError,
    > {
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
        let interest = crate::follow_index::FollowIndex::subscribe(&signals.follows, filter, tail)?;
        drop(state);
        let mut cursor =
            HistoryCursor::initial(snapshot.as_ref(), from, anchor, when_absent).await?;
        cursor.registration = Some(registration);
        cursor.end = selection.end;
        Ok((revision, selection, interest, cursor))
    }

    pub(crate) async fn next_checked_record(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
        anchor: &mut Option<CommitId>,
        filter: &crate::FollowFilter,
    ) -> Result<Option<Record>, StreamError> {
        if cursor.end.is_none() {
            let state = self.state.read().await;
            let snapshot = state.paths.get(path).cloned();
            drop(state);
            let path = snapshot.as_ref();
            if !cursor.observe_creation(path, *anchor).await? {
                return Ok(None);
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
        let mut examined = 0;
        while let Some(record) = cursor.next_record().await? {
            if anchor.is_some() {
                *anchor = Some(record.commit_id);
            }
            if let Some(record) = filter.project(record)? {
                return Ok(Some(record));
            }
            examined += 1;
            if examined == crate::REPLAY_PAGE {
                // Known finite snapshot work, chunked for cancellation and
                // fairness. No readiness or publication is rechecked here.
                crate::runtime::yield_now().await?;
                examined = 0;
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
        bootstrap_head: bool,
        until: Option<u64>,
    ) -> Result<crate::FollowStream, StreamError> {
        let (snapshot, selection, interest, cursor) = self
            .prepare_follow(
                &path,
                from,
                anchor,
                when_absent,
                filter.clone(),
                bootstrap_head,
                until,
            )
            .await?;
        let anchor = selection.preceding;
        let state = Box::new(MemoryFollow {
            provider: self.clone(),
            path,
            interest,
            filter,
            cursor,
            anchor,
            done: false,
        });
        Ok(crate::Opened::new(
            snapshot,
            selection,
            stream::unfold(state, |mut state| async move {
                if state.done || state.cursor.finished() {
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
                            if state.cursor.finished() {
                                return None;
                            }
                            if state.interest.changed.changed().await.is_err() {
                                break Err(if state.anchor.is_some() {
                                    StreamError::PrefixNotRetained
                                } else {
                                    StreamError::unavailable()
                                });
                            }
                        }
                    }
                };
                state.done = result.is_err();
                Some((result, state))
            })
            .boxed(),
        ))
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
