#[async_trait]
impl StreamProvider for LocalStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.visible(self.inner.provider.inspect_idempotency(idempotency_key))
            .await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.visible(self.inner.provider.tail(path)).await
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.visible(self.inner.provider.bounds(path)).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.append",
            items = request.records.len(),
            bytes = request
                .records
                .iter()
                .map(crate::OwnedBytes::len)
                .sum::<usize>(),
            rev = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            let operation = span.clone();
            self.exclusive(&span, move |stream| async move {
                let command = Command::Append(request.clone());
                let frame = stream.prepare(&command)?;
                let retain_conflict = request.idempotency_key.is_some();
                let outcome = stream.inner.provider.append(request).await?;
                if matches!(outcome, AppendOutcome::Committed(_)) || retain_conflict {
                    stream.persist(frame).await?;
                }
                if let AppendOutcome::Committed(receipt) = &outcome {
                    obs::record(&operation, "rev", receipt.tail);
                }
                Ok(outcome)
            })
            .await
        })
        .await
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.fork",
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            self.exclusive(&span, |stream| async move {
                let frame = stream.prepare(&Command::Fork(request.clone()))?;
                let outcome = stream.inner.provider.fork(request).await?;
                stream.persist(frame).await?;
                Ok(outcome)
            })
            .await
        })
        .await
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.read",
            rev = request.from,
            items = request.limit,
            outcome = Empty,
            error.kind = Empty
        ));
        let mut observation = ReadObservation {
            span,
            finished: false,
        };
        let result = observation.span.scope(self.read_visible(request)).await;
        observation.finished = true;
        obs::finish(&observation.span, result)
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.follow",
            rev = from,
            terminal = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            let mut state = FollowState {
                provider: self.clone(),
                path,
                next: from,
                changed: self.inner.changed.subscribe(),
                done: false,
                span: span.clone(),
            };
            if let Err(error) = self.tail(state.path.clone()).await {
                state.failed(&error);
                return Err(error);
            }
            Ok(span
                .scope(stream::unfold(state, |mut state| async move {
                    if state.done {
                        return None;
                    }
                    loop {
                        match state
                            .provider
                            .read_visible(ReadRequest {
                                path: state.path.clone(),
                                from: state.next,
                                limit: 1,
                            })
                            .await
                        {
                            Ok(mut records) => match records.next().await {
                                Some(Ok(record)) => {
                                    state.next = record.sequence.saturating_add(1);
                                    return Some((Ok(record), state));
                                }
                                Some(Err(error)) => {
                                    state.failed(&error);
                                    return Some((Err(error), state));
                                }
                                None => {}
                            },
                            Err(error) => {
                                state.failed(&error);
                                return Some((Err(error), state));
                            }
                        }
                        if state.changed.changed().await.is_err() {
                            state.failed(&StreamError::Unavailable);
                            return Some((Err(StreamError::Unavailable), state));
                        }
                    }
                }))
                .boxed())
        })
        .await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.visible(self.inner.provider.children(request)).await
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.visible(self.inner.provider.children_page(request))
            .await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.commit",
            items = request.mutations.len(),
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            self.exclusive(&span, |stream| async move {
                let frame = stream.prepare(&Command::Commit(request.clone()))?;
                let outcome = stream.inner.provider.commit(request).await?;
                stream.persist(frame).await?;
                Ok(outcome)
            })
            .await
        })
        .await
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.commit",
            items = request.mutations.len(),
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            self.exclusive(&span, move |stream| async move {
                let frame = stream.prepare(&Command::Commit(request.clone()))?;
                let outcome = stream
                    .inner
                    .provider
                    .commit_before(request, deadline_unix_millis)
                    .await?;
                stream.persist(frame).await?;
                Ok(outcome)
            })
            .await
        })
        .await
    }

    async fn read_commit(
        &self,
        commit_id: crate::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.visible(self.inner.provider.read_commit(commit_id))
            .await
    }
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, u64> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(
            &original,
            self.native_visible(self.inner.provider.native_tail(path, meter)),
        )
    }
    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, StreamBounds> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(
            &original,
            self.native_visible(self.inner.provider.native_bounds(path, meter)),
        )
    }
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::NativeRecordStream> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(
            &original,
            self.native_visible(self.inner.provider.native_read(request, meter)),
        )
    }
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(
            &original,
            self.native_visible(self.inner.provider.native_inspect_idempotency(key, meter)),
        )
    }
    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, AppendOutcome> {
        LocalStream::native_append(self, request, meter)
    }
    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome> {
        LocalStream::guarded_commit(self, request, None, meter, || {
            futures::future::ready(Ok(()))
        })
    }
    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome> {
        LocalStream::guarded_commit(self, request, Some(deadline), meter, || {
            futures::future::ready(Ok(()))
        })
    }
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: F,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome, E>
    where
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        LocalStream::guarded_commit(self, request, deadline, meter, before)
    }
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum: usize,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        self.prepare_commit(request, maximum, meter)
    }
}
