impl LocalStream {
    async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<crate::CommitId>,
    ) -> Result<RecordStream, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.follow",
            rev = from,
            terminal = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            let changed = self.inner.changed.subscribe();
            let prepared = self
                .accepted(self.inner.provider.prepare_follow(&path, from, anchor))
                .await
                .and_then(std::convert::identity);
            let (_, cursor) = prepared.inspect_err(|error| {
                span.record("terminal", "error");
                obs::failed(&span, error.code());
            })?;
            let state = FollowState {
                provider: self.clone(),
                path,
                cursor,
                anchor,
                changed,
                done: false,
                span: span.clone(),
            };
            Ok(span
                .scope(stream::try_unfold(state, |mut state| async move {
                    let result = async {
                        loop {
                            if let Some(record) = state
                                .provider
                                .accepted(state.provider.inner.provider.next_checked_record(
                                    &state.path,
                                    &mut state.cursor,
                                    &mut state.anchor,
                                ))
                                .await??
                            {
                                return Ok(record);
                            }
                            state.changed.changed().await.map_err(|_| {
                                if state.anchor.is_some() {
                                    StreamError::PrefixNotRetained
                                } else {
                                    StreamError::Unavailable
                                }
                            })?;
                        }
                    }
                    .await;
                    if let Err(error) = &result {
                        state.failed(error);
                    }
                    Ok(Some((result?, state)))
                }))
                .boxed())
        })
        .await
    }
}
