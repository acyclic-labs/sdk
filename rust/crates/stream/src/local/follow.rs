impl LocalStream {
    async fn follow_inner(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        let crate::FollowRequest {
            path,
            from,
            preceding,
            filter,
            progress,
            when_absent,
            at_least,
            bootstrap_head,
            until,
        } = request;
        // Park before visibility ownership, so a writer never waits on this floor.
        self.inner.provider.revisions.wait(&at_least).await?;
        if progress {
            self.progress_from(
                path,
                from,
                preceding,
                when_absent,
                filter,
                bootstrap_head,
                until,
            )
            .await
        } else {
            self.follow_from(
                path,
                from,
                preceding,
                when_absent,
                filter,
                bootstrap_head,
                until,
            )
            .await
        }
    }
    async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<crate::CommitId>,
        when_absent: crate::WhenAbsent,
        filter: crate::FollowFilter,
        bootstrap_head: bool,
        until: Option<u64>,
    ) -> Result<crate::FollowStream, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.follow.cursor",
            rev = from,
            terminal = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let (snapshot, selection, failed, changed, cursor) = {
            let prepare = std::pin::pin!(async {
                let failed = self.inner.failed.subscribe();
                let provider = match &self.work {
                    Some(work) => self.inner.provider.with_context(Arc::clone(work)),
                    None => self.inner.provider.clone(),
                };
                let prepared = self
                    .accepted(|| {
                        provider.prepare_follow(
                            &path,
                            from,
                            anchor,
                            when_absent,
                            filter.clone(),
                            bootstrap_head,
                            until,
                        )
                    })
                    .await
                    .and_then(std::convert::identity);
                let (snapshot, selection, changed, cursor) = prepared.inspect_err(|error| {
                    span.record("terminal", "error");
                    obs::failed(&span, error.code());
                })?;
                Ok::<_, StreamError>((snapshot, selection, failed, changed, cursor))
            });
            span.scope(prepare).await?
        };
        let anchor = selection.preceding;
        let allocation = self
            .work
            .as_ref()
            .map(|work| {
                work.record(acyclic_native_runtime::ReadAllocation::new(
                    &work.meter,
                    std::mem::size_of::<FollowState>(),
                ))
            })
            .transpose()?;
        let state = Box::new(FollowState {
            provider: self.clone(),
            path,
            cursor,
            anchor,
            changed,
            filter,
            failed,
            done: false,
            span: span.clone(),
            _allocation: allocation,
        });
        let cursor = span.scope(stream::try_unfold(state, |mut state| async move {
            loop {
                if state.cursor.finished() {
                    state.done = true;
                    state.span.record("terminal", "end");
                    return Ok(None);
                }
                #[cfg(loom)]
                let retain_pending = state.provider.inner.retain_pending_read;
                let (next, record) = crate::snapshot::read(
                    state,
                    #[cfg(loom)]
                    retain_pending,
                )
                .await;
                state = next;
                let result = match record {
                    Ok(Some(record)) => {
                        return Ok(Some((crate::FollowUpdate::Record(record), state)));
                    }
                    Ok(None) if state.cursor.finished() => {
                        state.done = true;
                        state.span.record("terminal", "end");
                        return Ok(None);
                    }
                    Ok(None) => state.wait().await,
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    state.failed(&error);
                    return Err(error);
                }
            }
        }));
        Ok(crate::Opened::new(
            snapshot,
            selection,
            crate::native::boxed_stream(cursor, self.work.as_ref())?,
        ))
    }
}
