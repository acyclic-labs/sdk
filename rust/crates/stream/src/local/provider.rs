use super::*;

#[async_trait]
impl StreamProvider for LocalStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.visible(self.inner.provider.inspect_idempotency(idempotency_key), None)
            .await.map_err(ordinary_failure)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.visible(self.inner.provider.tail(path), None).await.map_err(ordinary_failure)
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.visible(self.inner.provider.bounds(path), None).await.map_err(ordinary_failure)
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.mutation(MemoryOutput { value: Command::Append(request), allocation: Default::default() }, None, None, MutationVisibility::BeforeMutation, |provider, input, _, committed_at, _| async move {
            let Command::Append(request) = input.value else { unreachable!() };
            let retain_conflict = request.idempotency_key.is_some();
            let (outcome, error) = provider.apply_append(&request, None, Some(committed_at)).await?;
            let value = outcome.value;
            let persist = matches!(value, AppendOutcome::Committed(_)) || retain_conflict;
            Ok(CommitCompletion { value, error, persist, _input: None, visibility: None, primary_failure: false })
        }).await.map_err(ordinary_failure)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.mutation(MemoryOutput { value: Command::Fork(request), allocation: Default::default() }, None, None, MutationVisibility::BeforeMutation, |provider, input, _, _, _| async move {
            let Command::Fork(request) = input.value else { unreachable!() };
            let value = provider.fork(request).await?;
            Ok(CommitCompletion { value, error: None, persist: true, _input: None, visibility: None, primary_failure: false })
        }).await.map_err(ordinary_failure)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.read",
            rev = request.from,
            items = request.limit,
            outcome = Empty,
            error.kind = Empty
        ));
        let mut observation = obs::Cancellation(Some(span.clone()));
        let result = span.scope(self.visible(self.inner.provider.read(request), None)).await.map_err(ordinary_failure);
        drop(observation.0.take());
        obs::finish(&span, result)
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
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
                span: Some(span.clone()),
            };
            if let Err(error) = self.tail(state.path.clone()).await {
                state.failed(&error);
                return Err(error);
            }
            Ok(span
                .scope(stream::try_unfold(state, |mut state| async move {
                    let result = async {
                        loop {
                            let mut records = state.provider.visible(state.provider.inner.provider.read(ReadRequest {
                                path: state.path.clone(), from: state.next, limit: 1,
                            }), None).await.map_err(ordinary_failure)?;
                            if let Some(record) = records.next().await {
                                let record = record?;
                                state.next = record.sequence.saturating_add(1);
                                return Ok(record);
                            }
                            state.changed.changed().await.map_err(|_| StreamError::Unavailable)?;
                        }
                    }.await;
                    match result {
                        Ok(record) => Ok(Some((record, state))),
                        Err(error) => { state.failed(&error); Err(error) }
                    }
                }))
                .boxed())
        })
        .await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.visible(self.inner.provider.children(request), None).await.map_err(ordinary_failure)
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.visible(self.inner.provider.children_page(request), None)
            .await.map_err(ordinary_failure)
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.mutation(MemoryOutput { value: Command::Commit(request), allocation: Default::default() }, None, None, MutationVisibility::BeforeMutation, |provider, input, _, committed_at, _| async move {
            let Command::Commit(request) = input.value else { unreachable!() };
            let value = provider.commit_value(request, None, Some(committed_at)).await?;
            Ok(CommitCompletion { value, error: None, _input: None, visibility: None, persist: true, primary_failure: false })
        }).await.map_err(ordinary_failure)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.mutation(
            MemoryOutput { value: Command::Commit(request), allocation: Default::default() }, Some(deadline_unix_millis), None, MutationVisibility::BeforeMutation,
            |provider, input, deadline, committed_at, _| async move {
                let Command::Commit(request) = input.value else { unreachable!() };
                let value = provider.commit_value(request, deadline, Some(committed_at)).await?;
                Ok(CommitCompletion { value, error: None, _input: None, visibility: None, persist: true, primary_failure: false })
            },
        ).await.map_err(ordinary_failure)
    }

    async fn read_commit(
        &self,
        commit_id: crate::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.visible(self.inner.provider.read_commit(commit_id), None)
            .await.map_err(ordinary_failure)
    }

    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let result = self.visible(self.inner.provider.inspect_inner(key, Some(&meter)), Some(&meter)).await;
            crate::memory::native(result, &meter)
        })
    }

    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            let mut funding = crate::memory::MemoryOutput::fund(Some(&observer));
            let records = funding.records(&request.records)?;
            let input = funding.map(|_| Command::Append(AppendRequest {
                path: request.path.clone(), records, if_tail: request.if_tail,
                idempotency_key: request.idempotency_key.clone(),
            }));
            drop(request);
            let execute_meter = Arc::clone(&observer);
            let result = self.mutation(
                input, None, Some(observer), MutationVisibility::BeforeMutation,
                move |provider, input, _, committed_at, _| async move {
                    let MemoryOutput { value, allocation } = input;
                    let Command::Append(request) = value else { unreachable!() };
                    let retain_conflict = request.idempotency_key.is_some();
                    let (outcome, error) = provider.apply_append(
                        &request, Some(&execute_meter), Some(committed_at),
                    ).await?;
                    let value = crate::memory::native(Ok(outcome), &execute_meter)?
                        .expect("successful append completion");
                    let persist = matches!(value.value(), AppendOutcome::Committed(_))
                        || retain_conflict;
                    Ok(CommitCompletion {
                        value, error,
                        persist, _input: Some(MemoryOutput { value: Command::Append(request), allocation }), visibility: None, primary_failure: false,
                    })
                },
            ).await;
            native_result(result)
        })
    }

    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let result = self.visible(self.inner.provider.bounds_inner(path, Some(&meter)), Some(&meter)).await;
            let result = result.map(|output| crate::memory::MemoryOutput { value: output.value.tail, allocation: output.allocation });
            crate::memory::native(result, &meter)
        })
    }

    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, StreamBounds> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let result = self.visible(self.inner.provider.bounds_inner(path, Some(&meter)), Some(&meter)).await;
            crate::memory::native(result, &meter)
        })
    }

    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            let span = obs::OperationSpan::new(tracing::info_span!(
                "acyclic.stream.read",
                rev = request.from,
                items = request.limit,
                outcome = Empty,
                error.kind = Empty
            ));
            let mut observation = obs::Cancellation(Some(span.clone()));
            let result = span.scope(async {
                native_result(self.visible(async {
                    self.inner.provider.native_read(request, Arc::clone(&observer)).await
                        .map_err(MemoryFailure::Observer)?.map_err(MemoryFailure::Provider)
                }, Some(&observer)).await)
            }).await;
            drop(observation.0.take());
            finish_native(&span, result)
        })
    }

    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.native_commit_with_guard(request, None, meter, None::<fn() -> std::future::Ready<Result<(), StreamError>>>)
    }

    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
        before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        self.native_commit_with_guard(request, deadline_unix_millis, meter, Some(before))
    }

    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let result: MemoryResult<crate::PreparedNativeCommit> = async {
                self.inner.check_available()?;
                meter.check()?;
                let mut input = crate::capture_commit(&request, &meter)?;
                drop(request);
                let visibility = self.inner.visibility.write().await;
                self.inner.check_available()?;
                self.compact_if_due(Some(&meter)).await?;
                let prepared = self.inner.provider.prepare_commit(
                    &mut input.value, maximum_record_bytes, Arc::clone(&meter),
                ).await.map_err(MemoryFailure::Observer)?
                    .map_err(MemoryFailure::Provider)?;
                let recovery = self.inner.provider.prepare_recovery(&prepared)?;
                let input = input.map(Command::Commit);
                meter.admit_examination(1, 0)?;
                let maximum = crate::wire_codec::CanonicalWire::Journal(&input.value)
                    .encoded_len(Some(maximum_record_bytes), None)?;
                meter.record_examination(1, 0)?;
                meter.check()?;
                if maximum > MAX_COMMAND_BYTES { return Err(StreamError::LimitExceeded.into()); }
                let maximum_frame = maximum.checked_add(4 + FRAME_TIME_BYTES + FRAME_CHECKSUM_BYTES)
                    .ok_or(MemoryFailure::Provider(StreamError::Capacity))?;
                let frame = crate::memory::vec(Some(&meter), maximum_frame)?;
                self.inner.journal.lock()
                    .map_err(|_| MemoryFailure::Provider(self.inner.poison()))?
                    .admit_bytes(maximum_frame as u64).map_err(publication_error)?;
                meter.admit_examination(1, 0)?;
                meter.admit_codec(
                    maximum as u64, maximum_frame as u64,
                    (FRAME_DOMAIN.len() + maximum_frame - FRAME_CHECKSUM_BYTES) as u64,
                )?;
                let admission = AdmittedFrameWrite::new(&meter, maximum_frame)?.close_plan();
                let flush = self.plan_flush(Some(&meter))?;
                let persist = self.plan_persist(frame, Some(admission), Some(&meter))?;
                let slot = crate::NativeCommitRecordSlot::new(&meter)?;
                let received = Arc::clone(&slot);
                let mut synced = self.inner.synced.subscribe();
                let recovering = self.clone();
                let recovery = NativeProviderFuture::try_new(&meter, async move {
                    let visibility = recovering.inner.visibility.read().await;
                    if let Err(error) = recovering.inner.check_available() {
                        return Ok(Err(error));
                    }
                    let result = recovery.await;
                    let required = match recovering.inner.required() {
                        Ok(required) => required,
                        Err(error) => return Ok(Err(error)),
                    };
                    drop(visibility);
                    loop {
                        if let Err(error) = recovering.inner.check_available() {
                            return Ok(Err(error));
                        }
                        if *synced.borrow_and_update() >= required { return result; }
                        if synced.changed().await.is_err() {
                            return Ok(Err(StreamError::Unavailable));
                        }
                    }
                })?;
                let worker = fund_task(PreparedLocalWork {
                    persist, input, prepared, record: received,
                    runtime: tokio::runtime::Handle::current(),
                    meter: Arc::clone(&meter), flush,
                }, Some(&meter))?;
                drop(visibility);
                let worker = tokio::task::spawn_blocking(move || {
                    let result = worker.value.run();
                    drop(worker.allocation);
                    result
                });
                let future = match NativeProviderFuture::try_new(&meter, async move {
                    match worker.await {
                        Ok(result) => result,
                        Err(_) => Ok(Err(StreamError::Unavailable)),
                    }
                }) {
                    Ok(future) => future,
                    Err(error) => {
                        slot.close();
                        return Err(MemoryFailure::Observer(error));
                    }
                };
                Ok(crate::PreparedNativeCommit::new(future, recovery, slot, maximum_record_bytes))
            }.await;
            native_result(result.map(|prepared| NativeStreamOutput::new(
                prepared, ReadAllocation::empty(&meter),
            )))
        })
    }

    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.native_commit_with_guard(request, Some(deadline_unix_millis), meter, None::<fn() -> std::future::Ready<Result<(), StreamError>>>)
    }
}

struct FollowState {
    provider: LocalStream,
    path: StreamPath,
    next: u64,
    changed: watch::Receiver<u64>,
    span: Option<obs::OperationSpan>,
}
impl FollowState {
    fn failed(&mut self, error: &StreamError) {
        if let Some(span) = self.span.take() {
            span.record("terminal", "error");
            obs::failed(&span, error.code());
        }
    }
}
impl Drop for FollowState {
    fn drop(&mut self) {
        if let Some(span) = &self.span { span.record("terminal", "dropped"); }
    }
}
