#[async_trait]
impl StreamProvider for LocalStream {
    fn operation_claims(&self) -> &crate::OperationClaims {
        self.inner.provider.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.visible(self.inner.provider.inspect_idempotency(idempotency_key))
            .await?
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.visible(self.inner.provider.tail(path)).await?
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.visible(self.inner.provider.bounds(path)).await?
    }

    async fn watermarks(&self, path: StreamPath) -> Result<crate::StreamWatermarks, StreamError> {
        self.accepted(async {
            let accepted = self.inner.provider.tail(path.clone()).await?;
            // Both frame counters are published before publication is released.
            // Do not borrow the journal: compaction may hold it during device I/O.
            let synced = *self.inner.synced.borrow() >= *self.inner.revision.borrow();
            let mut tails = self
                .inner
                .durable_tails
                .lock()
                .map_err(|_| StreamError::Unavailable)?;
            let durable = tails.entry(path).or_default();
            if synced {
                *durable = (*durable).max(accepted);
            }
            Ok(crate::StreamWatermarks {
                accepted,
                durable: *durable,
            })
        })
        .await?
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        #[cfg(feature = "test-support")]
        acyclic_native_runtime::testing::describe(|| {
            format!("append {} {:?}", request.path, request.idempotency_key)
        });
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.append",
            items = request.records.len(),
            bytes = request
                .records
                .iter()
                .map(crate::AppendRecord::payload_bytes)
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
                    operation.record("rev", receipt.tail);
                }
                Ok(outcome)
            })
            .await
        })
        .await
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
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
        let span = obs::OperationSpan::new(tracing::info_span!(
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
        self.follow_from(path, from, None).await
    }

    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: crate::CommitId,
    ) -> Result<RecordStream, StreamError> {
        self.follow_from(path, from, Some(preceding)).await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.follow_children(request).await
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.visible(self.inner.provider.children_page(request))
            .await?
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        commit::<true>(self, request, None).await
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
    ) -> Result<CommitOutcome, StreamError> {
        commit::<true>(self, request, Some(deadline)).await
    }

    async fn read_commit(
        &self,
        commit_id: crate::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.visible(self.inner.provider.read_commit(commit_id))
            .await?
    }
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, u64> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            self.visible(self.inner.provider.native_tail(path, meter))
                .await
                .unwrap_or_else(|error| Ok(Err(error)))
        })
    }
    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, StreamBounds> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            self.visible(self.inner.provider.native_bounds(path, meter))
                .await
                .unwrap_or_else(|error| Ok(Err(error)))
        })
    }
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::NativeRecordStream> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            self.visible(self.inner.provider.native_read(request, meter))
                .await
                .unwrap_or_else(|error| Ok(Err(error)))
        })
    }
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            self.visible(self.inner.provider.native_inspect_idempotency(key, meter))
                .await
                .unwrap_or_else(|error| Ok(Err(error)))
        })
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
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome> {
        LocalStream::guarded_commit(self, request, deadline, meter, || {
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

async fn commit<const ELIDE_REPLAY: bool>(
    stream: &LocalStream,
    request: CommitRequest,
    deadline: Option<u64>,
) -> Result<CommitOutcome, StreamError> {
    #[cfg(feature = "test-support")]
    describe_commit(&request);
    let span = obs::OperationSpan::new(tracing::info_span!(
        "acyclic.stream.commit",
        items = request.mutations.len(),
        outcome = Empty,
        error.kind = Empty,
    ));
    span.scope(async {
        stream
            .exclusive(&span, move |stream| async move {
                // The visibility guard covers this lookup and settlement. A
                // replay still passes through commit_inner for fingerprint and
                // deadline semantics, then exclusive awaits the original WAL.
                let replay = ELIDE_REPLAY
                    && stream
                        .inner
                        .provider
                        .has_replay(&request.idempotency_key)
                        .await?;
                let frame = if replay {
                    None
                } else {
                    Some(stream.prepare(&Command::Commit(request.clone()))?)
                };
                let (outcome, _claim) = stream
                    .inner
                    .provider
                    .commit_inner(request, deadline)
                    .await?;
                if let Some(frame) = frame {
                    stream.persist(frame).await?;
                } else {
                    stream.require_accepted::<true>()?;
                }
                Ok(outcome)
            })
            .await
    })
    .await
}

#[cfg(feature = "test-support")]
fn describe_commit(request: &CommitRequest) {
    acyclic_native_runtime::testing::describe(|| {
        format!(
            "commit {:?}: {:?}",
            request.idempotency_key,
            request
                .mutations
                .iter()
                .map(|mutation| match mutation {
                    crate::CommitMutation::Append { path, .. } => path.as_str(),
                    crate::CommitMutation::Fork { destination, .. } => destination.as_str(),
                })
                .collect::<Vec<_>>()
        )
    });
}
