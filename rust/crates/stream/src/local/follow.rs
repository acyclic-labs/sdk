impl LocalStream {
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
            "acyclic.stream.follow",
            rev = from,
            terminal = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let (snapshot, selection, failed, changed, cursor) = {
            let prepare = std::pin::pin!(async {
                let failed = self.inner.failed.subscribe();
                let prepared = self
                    .accepted(self.inner.provider.prepare_follow(
                        &path,
                        from,
                        anchor,
                        when_absent,
                        filter.clone(),
                        bootstrap_head,
                        until,
                    ))
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
        });
        Ok(crate::Opened::new(snapshot, selection, span
            .scope(stream::try_unfold(state, |mut state| async move {
                loop {
                if state.cursor.finished() { state.done = true; state.span.record("terminal", "end"); return Ok(None); }
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
                        Ok(None) if state.cursor.finished() => { state.done = true; state.span.record("terminal", "end"); return Ok(None); },
                        Ok(None) => state.wait().await,
                        Err(error) => Err(error),
                    };
                    if let Err(error) = result {
                        state.failed(&error);
                        return Err(error);
                    }
                }
            }))
            .boxed()))
    }
}
