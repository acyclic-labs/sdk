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
    // A bounded follow is a read, which cannot start beyond the tail. Only a
    // live follow waits there for record `from`.
    if until.is_some() && snapshot.is_some_and(|path| from > path.tail) {
        return Err(StreamError::OutOfRange);
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
        let snapshot = self.observe_follow(|| state.paths.get(path).cloned())?;
        let revision = self.revisions.at(state.decision);
        let selection = follow_selection(snapshot.as_ref(), from, anchor, bootstrap_head, until)?;
        let (from, anchor) = (selection.from, selection.preceding);
        let tail = snapshot.as_ref().map_or(0, |path| path.tail);
        let registration = state.signals.subscribe_with(path.clone(), Signals::new)?;
        let signals = registration.value();
        let interest = crate::follow_index::FollowIndex::subscribe(&signals.follows, filter, tail)?;
        drop(state);
        let mut cursor =
            HistoryCursor::initial(snapshot.as_ref(), from, anchor, when_absent).await?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            cursor = cursor.with_native_work(self.work.clone());
        }
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
            let snapshot = cursor.observe(|| state.paths.get(path).cloned())?;
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
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = self
            .work
            .as_ref()
            .map(|work| {
                work.record(acyclic_native_runtime::ReadAllocation::new(
                    &work.meter,
                    std::mem::size_of::<MemoryFollow>(),
                ))
            })
            .transpose()?;
        let state = Box::new(MemoryFollow {
            provider: self.clone(),
            path,
            interest,
            filter,
            cursor,
            anchor,
            done: false,
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: allocation,
        });
        let cursor = stream::unfold(state, |mut state| async move {
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
                                StreamError::Unavailable
                            });
                        }
                    }
                }
            };
            state.done = result.is_err();
            Some((result, state))
        });
        #[cfg(not(target_arch = "wasm32"))]
        let cursor = crate::native::boxed_stream(cursor, self.work.as_ref())?;
        #[cfg(target_arch = "wasm32")]
        let cursor = cursor.boxed_local();
        Ok(crate::Opened::new(snapshot, selection, cursor))
    }
}

impl MemoryStream {
    fn observe_follow<T>(&self, lookup: impl FnOnce() -> T) -> Result<T, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work.record(inspect(&work.meter, lookup));
        }
        Ok(lookup())
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
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
impl crate::snapshot::Snapshot for Box<MemoryFollow> {
    type Output = Option<Record>;
    fn caller_retains_pending(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.provider.work.is_some()
        }
        #[cfg(target_arch = "wasm32")]
        {
            false
        }
    }
    async fn read(&mut self) -> Result<Self::Output, StreamError> {
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
