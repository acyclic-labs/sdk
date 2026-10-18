// Each path is linked exactly once into its parent's creation index. The link
// storage is part of PathState and its existing native allocation charge.
struct ChildIndex {
    first: Option<StreamPath>,
    last: Option<StreamPath>,
    changed: watch::Sender<()>,
}
impl Default for ChildIndex {
    fn default() -> Self {
        Self {
            first: None,
            last: None,
            changed: watch::channel(()).0,
        }
    }
}
impl State {
    fn child_index(&self, parent: Option<&StreamPath>) -> Option<&ChildIndex> {
        match parent {
            Some(parent) => self.paths.get(parent).map(|path| &path.children),
            None => Some(&self.roots),
        }
    }
    fn index_child(&mut self, path: StreamPath) -> Result<(), StreamError> {
        let parent = path.parent();
        let index = match &parent {
            Some(parent) => {
                &mut self
                    .paths
                    .get_mut(parent)
                    .ok_or(StreamError::Unavailable)?
                    .children
            }
            None => &mut self.roots,
        };
        let previous = index.last.replace(path.clone());
        if index.first.is_none() {
            index.first = Some(path.clone());
        }
        index.changed.send_replace(());
        if let Some(previous) = previous {
            self.paths
                .get_mut(&previous)
                .ok_or(StreamError::Unavailable)?
                .next_sibling = Some(path);
        }
        Ok(())
    }
    fn next_child(
        &self,
        parent: Option<&StreamPath>,
        previous: Option<&StreamPath>,
    ) -> Result<Option<StreamPath>, StreamError> {
        #[cfg(test)]
        self.child_visits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match previous {
            Some(previous) => self
                .paths
                .get(previous)
                .map(|path| path.next_sibling.clone())
                .ok_or(StreamError::Unavailable),
            None => Ok(self
                .child_index(parent)
                .and_then(|index| index.first.clone())),
        }
    }
}
impl MemoryStream {
    async fn follow_children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        validate_limit(request.limit)?;
        let changed = self.child_changes(request.parent.as_ref()).await;
        let state = Box::new(MemoryChildren {
            provider: self.clone(),
            parent: request.parent,
            previous: None,
            changed,
        });
        Ok(stream::try_unfold(state, |mut state| async move {
            loop {
                #[cfg(acyclic_loom)]
                let retain_pending = state.provider.retain_pending_read;
                let (next, child) = crate::snapshot::read(
                    state,
                    #[cfg(acyclic_loom)]
                    retain_pending,
                )
                .await;
                state = next;
                if let Some(path) = child? {
                    state.previous = Some(path.clone());
                    return Ok(Some((Child { path }, state)));
                }
                state
                    .changed
                    .changed()
                    .await
                    .map_err(|_| StreamError::Unavailable)?;
            }
        })
        .boxed())
    }
    #[cfg(acyclic_loom)]
    pub(crate) async fn model_hold_writer(&self) -> impl Send + '_ {
        self.state.write().await
    }
    pub(crate) async fn child_changes(&self, parent: Option<&StreamPath>) -> watch::Receiver<()> {
        let mut state = self.state.write().await;
        if let Some(index) = state.child_index(parent) {
            return index.changed.subscribe();
        }
        state
            .hierarchy_changed
            .get_or_insert_with(|| watch::channel(()).0)
            .subscribe()
    }
    pub(crate) async fn next_child_snapshot(
        &self,
        parent: Option<&StreamPath>,
        previous: Option<&StreamPath>,
        changed: &mut watch::Receiver<()>,
    ) -> Result<Option<StreamPath>, StreamError> {
        let state = self.state.read().await;
        // Switch from the creation wakeup to this parent's own index.
        if let Some(index) = state.child_index(parent) {
            *changed = index.changed.subscribe();
        }
        changed.borrow_and_update();
        state.next_child(parent, previous)
    }
    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub(crate) async fn next_child(
        &self,
        parent: Option<&StreamPath>,
        previous: Option<&StreamPath>,
    ) -> Result<Option<StreamPath>, StreamError> {
        self.state.read().await.next_child(parent, previous)
    }
}

struct MemoryChildren {
    provider: MemoryStream,
    parent: Option<StreamPath>,
    previous: Option<StreamPath>,
    changed: watch::Receiver<()>,
}
impl crate::snapshot::Snapshot for Box<MemoryChildren> {
    type Output = Result<Option<StreamPath>, StreamError>;
    async fn read(&mut self) -> Self::Output {
        let state = &mut **self;
        state
            .provider
            .next_child_snapshot(
                state.parent.as_ref(),
                state.previous.as_ref(),
                &mut state.changed,
            )
            .await
    }
}
