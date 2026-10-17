use super::*;

struct ChildrenState {
    provider: LocalStream,
    parent: Option<StreamPath>,
    previous: Option<StreamPath>,
    changed: watch::Receiver<()>,
    failed: watch::Receiver<bool>,
}
impl crate::snapshot::Snapshot for Box<ChildrenState> {
    const OBSERVE: bool = true;
    type Output = Result<Option<StreamPath>, StreamError>;
    async fn read(&mut self) -> Self::Output {
        let state = &mut **self;
        state
            .provider
            .accepted_snapshot(state.provider.inner.provider.next_child_snapshot(
                state.parent.as_ref(),
                state.previous.as_ref(),
                &mut state.changed,
            ))
            .await?
    }
}

impl LocalStream {
    pub(super) async fn follow_children(
        &self,
        request: ChildrenRequest,
    ) -> Result<ChildStream, StreamError> {
        crate::memory::validate_limit(request.limit)?;
        let failed = self.inner.failed.subscribe();
        let changed = self
            .accepted(self.inner.provider.child_changes(request.parent.as_ref()))
            .await?;
        let state = Box::new(ChildrenState {
            provider: self.clone(),
            parent: request.parent,
            previous: None,
            changed,
            failed,
        });
        Ok(stream::try_unfold(state, |mut state| async move {
            loop {
                #[cfg(acyclic_loom)]
                let retain_pending = state.provider.inner.retain_pending_read;
                let (next, child) = crate::snapshot::read(
                    state,
                    #[cfg(acyclic_loom)]
                    retain_pending,
                )
                .await;
                state = next;
                if let Some(path) = child? {
                    state.previous = Some(path.clone());
                    return Ok(Some((crate::Child { path }, state)));
                }
                let unavailable = LocalInner::unavailable(&mut state.failed);
                let changed = state.changed.changed();
                futures::pin_mut!(unavailable, changed);
                match futures::future::select(unavailable, changed).await {
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
