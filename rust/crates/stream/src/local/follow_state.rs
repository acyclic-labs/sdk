struct FollowState {
    provider: LocalStream,
    path: StreamPath,
    cursor: crate::memory::HistoryCursor,
    anchor: Option<crate::CommitId>,
    changed: crate::follow_index::FollowInterest,
    filter: crate::FollowFilter,
    failed: watch::Receiver<Option<LocalStreamFailure>>,
    done: bool,
    span: obs::OperationSpan,
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
impl crate::snapshot::Snapshot for Box<FollowState> {
    #[cfg(feature = "test-support")]
    const OBSERVE: bool = true;
    type Output = Option<crate::Record>;
    fn caller_retains_pending(&self) -> bool {
        self.provider.work.is_some()
    }
    async fn read(&mut self) -> Result<Self::Output, StreamError> {
        let state = &mut **self;
        state
            .provider
            .accepted_snapshot(|| {
                state.provider.inner.provider.next_checked_record(
                    &state.path,
                    &mut state.cursor,
                    &mut state.anchor,
                    &state.filter,
                )
            })
            .await?
    }

    async fn read_retained(&mut self) -> Result<Option<crate::Record>, StreamError> {
        let state = &mut **self;
        let provider = state
            .provider
            .inner
            .provider
            .clone()
            .with_unreserved_reads(false);
        state
            .provider
            .retained_snapshot(provider.next_checked_record(
                &state.path,
                &mut state.cursor,
                &mut state.anchor,
                &state.filter,
            ))
            .await?
    }
}
impl FollowState {
    async fn wait(&mut self) -> Result<(), StreamError> {
        // The follow's address identifies it while it waits.
        let key = std::ptr::from_ref(&*self).addr();
        let publication = self.changed.changed.changed();
        #[cfg(loom)]
        let publication = async {
            if self.provider.inner.omit_follow_wake {
                std::future::pending::<()>().await;
            }
            publication.await
        };
        let unavailable = LocalInner::unavailable(&mut self.failed);
        let publication = core::pin::pin!(publication);
        let unavailable = core::pin::pin!(unavailable);
        let request = crate::trace::Request {
            name: "stream.local_follow_wait",
            mechanism: crate::trace::Mechanism::Wait,
            parent: None,
            guarantee: Some("local_publication_or_unavailability"),
            key: u64::try_from(key).ok().map(crate::trace::Key::Sequence),
        };
        let ready = crate::trace::observe_value(
            &request,
            futures::future::select(unavailable, publication),
            |ready| match ready {
                futures::future::Either::Right((Ok(_), _)) => {
                    Some((Some(0), crate::trace::Outcome::Succeeded))
                }
                _ => None,
            },
        );
        match ready.await {
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
