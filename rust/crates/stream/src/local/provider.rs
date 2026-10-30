#[async_trait]
impl StreamProvider for LocalStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(self.inner.provider.context().operation_claims())
            .with_process_exclusion()
            .with_deferred_durability()
    }

    async fn durability_barrier(&self) -> Result<(), StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.durability_barrier",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async { self.seal_accepted().await }
        )
    }

    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.inspect_idempotency",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.accepted(
                    self.inner
                        .provider
                        .inspect_idempotency_inner(idempotency_key),
                )
                .await?
            }
        )
    }

    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.heads",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                request.validate()?;
                self.inner
                    .provider
                    .revisions
                    .wait(&request.at_least)
                    .await?;
                let projection = request.projection;
                let through = request.durable_through;
                let (mut view, revision) = self
                    .accepted(async {
                        let mut view = self.inner.provider.capture_heads(&request, false).await?;
                        let revision = *self.inner.revision.borrow();
                        if projection == crate::HeadProjection::Tail || request.paths.is_empty() {
                            return Ok::<_, StreamError>((view, revision));
                        }
                        let synced = self
                            .inner
                            .synced
                            .borrow()
                            .flushed
                            .is_some_and(|cut| cut >= revision);
                        let mut tails = self
                            .inner
                            .durable_tails
                            .lock()
                            .map_err(|_| StreamError::Unavailable)?;
                        for (path, head) in request.paths.iter().zip(&mut view.heads) {
                            if let Some(head) = head {
                                let mut durable = tails.get(path).copied().unwrap_or(0);
                                if synced && head.tail > durable {
                                    durable = head.tail;
                                    tails.insert(path.clone(), durable);
                                }
                                head.durable = durable.min(head.tail);
                                head.retained_through =
                                    through.filter(|through| *through <= head.durable);
                            }
                        }
                        Ok((view, revision))
                    })
                    .await??;
                if through.is_some_and(|through| {
                    view.heads
                        .iter()
                        .flatten()
                        .any(|head| through <= head.tail && head.retained_through.is_none())
                }) {
                    // Flush exactly the captured publication cut, outside its read lock.
                    // Concurrent appends cannot change the view or enlarge its proof.
                    let flushed = self.await_flushed(revision).await?;
                    let mut tails = self
                        .inner
                        .durable_tails
                        .lock()
                        .map_err(|_| StreamError::Unavailable)?;
                    for (path, head) in request.paths.into_iter().zip(&mut view.heads) {
                        if let Some(head) = head {
                            let durable = tails.entry(path).or_default();
                            let marks = flushed_watermarks(durable, head.tail, flushed);
                            head.durable = marks.durable;
                            head.retained_through =
                                through.filter(|through| *through <= marks.durable);
                        }
                    }
                }
                Ok(view)
            }
        )
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        #[cfg(feature = "test-support")]
        match &request {
            crate::WriteRequest::Append(request) => {
                acyclic_native_runtime::testing::describe(|| {
                    format!("append {} {:?}", request.path, request.idempotency_key)
                })
            }
            crate::WriteRequest::Commit(request) => describe_commit(request),
            crate::WriteRequest::Fork(_) => {}
        }
        let span = obs::OperationSpan::new(match &request {
            crate::WriteRequest::Append(request) => tracing::info_span!(
                "acyclic.stream.append",
                items = request.records.len(),
                bytes = request
                    .records
                    .iter()
                    .map(crate::AppendRecord::payload_bytes)
                    .sum::<usize>(),
                rev = Empty,
                outcome = Empty,
                error.kind = Empty,
            ),
            crate::WriteRequest::Fork(_) => {
                tracing::info_span!("acyclic.stream.fork", outcome = Empty, error.kind = Empty,)
            }
            crate::WriteRequest::Commit(request) => tracing::info_span!(
                "acyclic.stream.commit",
                items = request.mutations.len(),
                outcome = Empty,
                error.kind = Empty,
            ),
        });
        let operation = span.clone();
        Box::pin(self.exclusive(span, move |stream| async move {
            let outcome = stream.execute(request, None).await?.outcome;
            if let crate::WriteOutcome::Append(AppendOutcome::Committed(receipt)) = &outcome {
                operation.record("rev", receipt.tail);
            }
            Ok(outcome)
        }))
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.read",
            rev = request.from,
            items = request.limit,
            outcome = Empty,
            error.kind = Empty
        ));
        let completion = acyclic_observability::Completion::new(&span).on_drop_error("cancelled");
        let result = span
            .scope(async {
                // Parks before any visibility guard, so no writer waits on a parked read.
                self.inner
                    .provider
                    .revisions
                    .wait(&request.at_least)
                    .await?;
                self.accepted(self.inner.provider.read_inner(request))
                    .await?
            })
            .await;
        completion.finish(result, StreamError::code)
    }

    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.follow",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let crate::FollowRequest {
                    path,
                    from,
                    preceding,
                    filter,
                    progress,
                    when_absent,
                    at_least,
                } = request;
                // Parks before any visibility guard, so no writer waits on a parked read.
                self.inner.provider.revisions.wait(&at_least).await?;
                if progress {
                    self.progress_from(path, from, preceding, when_absent, filter)
                        .await
                } else {
                    self.follow_from(path, from, preceding, when_absent, filter)
                        .await
                }
            }
        )
    }

    async fn read_commit(
        &self,
        commit_id: crate::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.read_commit",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.check_available()?;
                match self.inner.provider.read_commit_inner(commit_id).await {
                    Err(StreamError::NotFound) => {}
                    result => return result,
                }
                // A retained envelope is immutable. Only absence must join publication
                // before it can be reported as authoritative.
                self.accepted(self.inner.provider.read_commit_inner(commit_id))
                    .await?
            }
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::NativeStreamProvider for LocalStream {
    fn native_durability_barrier(
        &self,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, ()> {
        let stream = self.clone();
        crate::native::Work::start(meter, |work| async move {
            let stream = Self {
                work: Some(Arc::clone(&work)),
                ..stream
            };
            let result = async {
                stream.seal_accepted().await?;
                Ok(((), None))
            }
            .await;
            work.finish(result)
        })
    }
    fn prepare_native_durability_barrier(
        &self,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<crate::NativeProviderFuture<'_, ()>> {
        let work = crate::native::Work::new(Arc::clone(&meter))?;
        // This ticket survives original cancellation and excludes competing
        // work from consuming the phase's final durability allowance.
        work.prepare_durability(1)?;
        let stream = Self {
            work: Some(Arc::clone(&work)),
            ..self.clone()
        };
        crate::NativeProviderFuture::try_new(&meter, async move {
            let result = stream.seal_accepted_with_reservation(true).await;
            work.finish(result.map(|()| ((), None)))
        })
    }
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, u64> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            self.accepted(self.inner.provider.native_tail(path, meter))
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
            if let Err(error) = self.inner.provider.revisions.wait(&request.at_least).await {
                return Ok(Err(error));
            }
            self.accepted(self.inner.provider.native_read(request, meter))
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
            self.accepted(self.inner.provider.native_inspect_idempotency(key, meter))
                .await
                .unwrap_or_else(|error| Ok(Err(error)))
        })
    }
    fn native_write(
        &self,
        request: crate::WriteRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::WriteOutcome> {
        match request {
            crate::WriteRequest::Append(request) => {
                self.append_funded(request, meter, crate::WriteOutcome::Append)
            }
            crate::WriteRequest::Commit(request) => self.guarded_commit(
                request,
                None,
                meter,
                || futures::future::ready(Ok(())),
                crate::WriteOutcome::Commit,
            ),
            crate::WriteRequest::Fork(_) => {
                crate::NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
            }
        }
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
        LocalStream::guarded_commit(
            self,
            request,
            deadline,
            meter,
            before,
            std::convert::identity,
        )
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

#[cfg(loom)]
impl LocalStream {
    fn receipt_for_flushed_snapshot(
        &self,
        path: StreamPath,
        accepted: u64,
        through: u64,
        _flushed: DeviceFlushed,
    ) -> Result<Option<crate::DurableReceipt>, StreamError> {
        // No other lock is held here. Watermark reads acquire publication first,
        // then durable_tails; this path never acquires publication afterward.
        let mut tails = self
            .inner
            .durable_tails
            .lock()
            .map_err(|_| StreamError::Unavailable)?;
        let durable = tails.entry(path.clone()).or_default();
        let marks = flushed_watermarks(durable, accepted, _flushed);
        Ok(crate::DurableReceipt::covering(path, &marks, through))
    }
}

// Pair a completed device flush with its captured accepted cut, never a newer
// cached watermark. The cache can advance independently after this observation.
fn flushed_watermarks(
    durable: &mut u64,
    accepted: u64,
    _flushed: DeviceFlushed,
) -> crate::StreamWatermarks {
    *durable = (*durable).max(accepted);
    let covered = accepted;
    #[cfg(loom)]
    let covered = if crate::verification::fault_is(crate::verification::Fault::MixedReceiptCut) {
        *durable
    } else {
        covered
    };
    crate::StreamWatermarks {
        accepted,
        durable: covered,
    }
}
