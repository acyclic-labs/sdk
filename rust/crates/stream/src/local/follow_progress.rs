impl LocalStream {
    async fn progress_from(
        &self,
        path: StreamPath,
        from: u64,
        anchor: Option<crate::CommitId>,
        filter: crate::FollowFilter,
    ) -> Result<crate::FollowStream, StreamError> {
        let failed = self.inner.failed.subscribe();
        let (changed, cursor) = self
            .accepted(self.inner.provider.prepare_progress(&path, from, anchor))
            .await??;
        let state = Box::new(LocalProgress {
            provider: self.clone(),
            path,
            cursor,
            anchor,
            filter,
            changed,
            failed,
        });
        Ok(stream::try_unfold(state, |mut state| async move {
            loop {
                #[cfg(acyclic_loom)]
                let retain_pending = state.provider.inner.retain_pending_read;
                let (next, update) = crate::snapshot::read(
                    state,
                    #[cfg(acyclic_loom)]
                    retain_pending,
                )
                .await;
                state = next;
                if let Some(update) = update? {
                    return Ok(Some((update, state)));
                }
                let changed = state.changed.changed();
                let failed = LocalInner::unavailable(&mut state.failed);
                futures::pin_mut!(changed, failed);
                match futures::future::select(failed, changed).await {
                    futures::future::Either::Left((error, _)) => return Err(error),
                    futures::future::Either::Right((result, _)) => {
                        result.map_err(|_| StreamError::Unavailable)?
                    }
                }
            }
        })
        .boxed())
    }
}
struct LocalProgress {
    provider: LocalStream,
    path: StreamPath,
    cursor: crate::memory::HistoryCursor,
    anchor: Option<crate::CommitId>,
    filter: crate::FollowFilter,
    changed: watch::Receiver<u64>,
    failed: watch::Receiver<bool>,
}
impl crate::snapshot::Snapshot for Box<LocalProgress> {
    #[cfg(feature = "test-support")]
    const OBSERVE: bool = true;
    type Output = Result<Option<crate::FollowUpdate>, StreamError>;
    async fn read(&mut self) -> Self::Output {
        let state = &mut **self;
        state
            .provider
            .accepted_snapshot(state.provider.inner.provider.next_follow_update(
                &state.path,
                &mut state.cursor,
                &mut state.anchor,
                &state.filter,
            ))
            .await?
    }
}
