struct FollowState {
    provider: LocalStream,
    path: StreamPath,
    cursor: crate::memory::HistoryCursor,
    anchor: Option<crate::CommitId>,
    changed: watch::Receiver<u64>,
    failed: watch::Receiver<bool>,
    done: bool,
    span: obs::OperationSpan,
}
impl FollowState {
    async fn wait(&mut self) -> Result<(), StreamError> {
        let publication = self.changed.changed();
        #[cfg(acyclic_loom)]
        let publication = async {
            if self.provider.inner.omit_follow_wake {
                std::future::pending::<()>().await;
            }
            publication.await
        };
        let unavailable = LocalInner::unavailable(&mut self.failed);
        futures::pin_mut!(publication, unavailable);
        match futures::future::select(unavailable, publication).await {
            futures::future::Either::Left((error, _)) => Err(error),
            futures::future::Either::Right((result, _)) => result.map_err(|_| {
                if self.anchor.is_some() {
                    StreamError::PrefixNotRetained
                } else {
                    StreamError::Unavailable
                }
            }),
        }
    }

    fn failed(&mut self, error: &StreamError) {
        self.done = true;
        self.span.record("terminal", "error");
        obs::failed(&self.span, error.code());
    }
}
impl Drop for FollowState {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("terminal", "dropped");
        }
    }
}
