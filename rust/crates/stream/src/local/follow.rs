impl LocalStream {
    async fn follow_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<crate::CommitId>,
        filter: crate::FollowFilter,
    ) -> Result<crate::FollowStream, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.follow",
            rev = from,
            terminal = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let (failed, changed, cursor) = span
            .scope(async {
                let failed = self.inner.failed.subscribe();
                let prepared = self
                    .accepted(self.inner.provider.prepare_follow(
                        &path,
                        from,
                        anchor,
                        filter.clone(),
                    ))
                    .await
                    .and_then(std::convert::identity);
                let (changed, cursor) = prepared.inspect_err(|error| {
                    span.record("terminal", "error");
                    obs::failed(&span, error.code());
                })?;
                Ok::<_, StreamError>((failed, changed, cursor))
            })
            .await?;
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
        Ok(span
            .scope(stream::try_unfold(state, |mut state| async move {
                loop {
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
                        Ok(None) => state.wait().await,
                        Err(error) => Err(error),
                    };
                    if let Err(error) = result {
                        state.failed(&error);
                        return Err(error);
                    }
                }
            }))
            .boxed())
    }
}
