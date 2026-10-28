#[async_trait]
impl StreamProvider for LocalStream {
    fn has_exclusive_process_owner(&self) -> bool {
        true
    }
    fn operation_claims(&self) -> &crate::OperationClaims {
        self.inner.provider.operation_claims()
    }

    fn supports_deferred_durability(&self) -> bool {
        true
    }

    async fn durability_barrier(&self) -> Result<(), StreamError> {
        self.seal_accepted().await
    }

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

    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.visible(self.inner.provider.inspect_idempotency(idempotency_key))
            .await?
    }

    async fn head(&self, path: StreamPath) -> Result<crate::PathHead, StreamError> {
        self.accepted(async {
            let mut head = self.inner.provider.head(path.clone()).await?;
            head.durable = self.durable_tail_for_accepted(path, head.tail)?;
            Ok(head)
        })
        .await?
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.visible(self.inner.provider.tail(path)).await?
    }

    async fn watermarks(&self, path: StreamPath) -> Result<crate::StreamWatermarks, StreamError> {
        self.accepted(async {
            let accepted = self.inner.provider.tail(path.clone()).await?;
            Ok(crate::StreamWatermarks {
                accepted,
                durable: self.durable_tail_for_accepted(path, accepted)?,
            })
        })
        .await?
    }

    /// The explicit durability barrier: frames written under
    /// `deferring_durability` are flushed here before any receipt covers them.
    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<crate::DurableReceipt>, StreamError> {
        let observed = self.watermarks(path.clone()).await?;
        if let Some(receipt) = crate::DurableReceipt::covering(path.clone(), &observed, through) {
            return Ok(Some(receipt));
        }
        // Every frame behind this path's accepted tail is at or before the
        // published revision observed under the same publication guard.
        let (accepted, revision) = self
            .accepted(async {
                let accepted = self.inner.provider.tail(path.clone()).await?;
                Ok::<_, StreamError>((accepted, *self.inner.revision.borrow()))
            })
            .await??;
        let flushed = self.await_flushed(revision).await?;
        self.receipt_for_flushed_snapshot(path, accepted, through, flushed)
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
                let replay = stream
                    .inner
                    .provider
                    .has_replay(&request.idempotency_key)
                    .await?;
                let frame = if replay {
                    None
                } else {
                    Some(stream.prepare(&Command::Append(request.clone()))?)
                };
                let outcome = stream.inner.provider.append(request).await?;
                if let Some(frame) = frame
                    && matches!(outcome, AppendOutcome::Committed(_))
                {
                    stream.persist(frame).await?;
                } else if replay {
                    stream.require_accepted::<true>()?;
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

    async fn wait_applied(
        &self,
        at_least: &crate::RevisionCut,
    ) -> Result<crate::Revision, StreamError> {
        self.inner.provider.revisions.wait(at_least).await
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.fork",
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            self.exclusive(&span, |stream| async move {
                let replay = stream
                    .inner
                    .provider
                    .has_replay(&request.idempotency_key)
                    .await?;
                let frame = if replay {
                    None
                } else {
                    Some(stream.prepare(&Command::Fork(request.clone()))?)
                };
                let outcome = stream.inner.provider.fork(request).await?;
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
        let result = observation
            .span
            .scope(async {
                // Parks before any visibility guard, so no writer waits on a parked read.
                self.inner
                    .provider
                    .revisions
                    .wait(&request.at_least)
                    .await?;
                self.read_visible(request).await
            })
            .await;
        observation.finished = true;
        obs::finish(&observation.span, result)
    }

    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
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
                if let Some(frame) = frame
                    && matches!(outcome, CommitOutcome::Committed(_))
                {
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

impl LocalStream {
    // Called under accepted's publication guard after observing this path's tail.
    fn durable_tail_for_accepted(
        &self,
        path: StreamPath,
        accepted: u64,
    ) -> Result<u64, StreamError> {
        // Both frame counters are published before publication is released.
        // Do not borrow the journal: compaction may hold it during device I/O.
        let synced = self
            .inner
            .synced
            .borrow()
            .flushed
            .is_some_and(|cut| cut >= *self.inner.revision.borrow());
        let mut tails = self
            .inner
            .durable_tails
            .lock()
            .map_err(|_| StreamError::Unavailable)?;
        let durable = tails.entry(path).or_default();
        if synced {
            *durable = (*durable).max(accepted);
        }
        Ok(*durable)
    }

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
        *durable = (*durable).max(accepted);
        // This call flushed this exact snapshot. Later callers may have advanced
        // durable_tails beyond it; never pair that newer prefix with this cut.
        let covered = accepted;
        #[cfg(loom)]
        let covered = if crate::verification::fault_is(crate::verification::Fault::MixedReceiptCut)
        {
            *durable
        } else {
            covered
        };
        let marks = crate::StreamWatermarks {
            accepted,
            durable: covered,
        };
        Ok(crate::DurableReceipt::covering(path, &marks, through))
    }
}
