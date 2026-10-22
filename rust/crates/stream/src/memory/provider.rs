#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StreamProvider for MemoryStream {
    fn operation_claims(&self) -> &crate::OperationClaims {
        &self.operation_claims
    }

    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let state = self.state.read().await;
        Ok(self
            .replay(&state, idempotency_key.as_bytes())?
            .map(|replay| IdempotencyObservation {
                idempotency_key,
                request_digest: replay.digest,
                outcome: replay.result.clone(),
            }))
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        let state = self.state.read().await;
        state
            .paths
            .get(&path)
            .map(|stream| stream.tail)
            .ok_or(StreamError::NotFound)
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        let state = self.state.read().await;
        let stream = state.paths.get(&path).ok_or(StreamError::NotFound)?;
        Ok(StreamBounds { tail: stream.tail })
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.append_inner(request).await.map(|(value, _)| value)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let digest = fork_digest(&request)?;
        let mut state = self.state.write().await;
        if let Some(result) = self.replay_fork(&state, request.idempotency_key.as_ref(), digest)? {
            return Ok(result);
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        let mut probes = self.reserve_replay_probes(
            request
                .idempotency_key
                .as_ref()
                .map_or(0, |_| u64::from(state.replay_height(state.replay_root))),
        )?;
        let forked_at = crate::preparation::fork_cut(
            &request,
            state.paths.get(&request.source).map(|path| path.tail),
            state.paths.get(&request.destination).map(|path| path.tail),
        )?;
        reserve_paths(&state, &request.destination, self.limits)?;
        reserve_commit(&state, self.limits)?;
        let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
        let commit_id = commit_identity(decision, digest);
        let receipt = ForkReceipt {
            source: request.source,
            destination: request.destination,
            forked_at,
            tail: forked_at,
            commit_id,
        };
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Fork(CommittedFork {
                source: receipt.source.clone(),
                destination: receipt.destination.clone(),
                forked_at,
                tail: forked_at,
                records: Vec::new(),
            })],
        };
        let (histories, _histories) = self.histories(&state, &envelope.mutations)?;
        let (paths, _paths) = self.paths(&state, std::iter::once(&receipt.destination))?;
        self.reserve_retained(&mut state)?;
        publish(&mut state, paths, histories, &envelope)?;
        state.decision = decision;
        retain(
            &mut state,
            &mut || self.record_replay_probe(&mut probes),
            Some(envelope),
            request.idempotency_key,
            digest,
            IdempotencyOutcome::Fork(receipt.clone()),
            Default::default(),
        );
        Ok(receipt)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        validate_limit(request.limit)?;
        let state = self.state.read().await;
        let path = state
            .paths
            .get(&request.path)
            .ok_or(StreamError::NotFound)?;
        if request.from > path.tail {
            return Err(StreamError::OutOfRange);
        }
        let end = path
            .tail
            .min(request.from.saturating_add(u64::from(request.limit)));
        let mut cursor = HistoryCursor::new(path, end, request.from);
        Ok(stream::iter(
            std::iter::from_fn(move || cursor.next_record())
                .take(usize::try_from(end - request.from).unwrap_or(usize::MAX))
                .map(Ok),
        )
        .boxed())
    }

    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.follow_from(path, from, None, filter).await
    }

    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.follow_from(path, from, Some(preceding), filter).await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.follow_children(request).await
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        validate_limit(request.limit)?;
        if request.after.is_some() && request.hierarchy_version.is_none() {
            return Err(StreamError::InvalidArgument);
        }
        if request.after.as_ref().is_some_and(|after| {
            !request.parent.as_ref().map_or_else(
                || !after.as_str().contains('/'),
                |parent| after.parent().as_ref() == Some(parent),
            )
        }) {
            return Err(StreamError::InvalidArgument);
        }
        let state = self.state.read().await;
        if request
            .hierarchy_version
            .is_some_and(|version| version != state.hierarchy_version)
        {
            return Err(StreamError::HierarchyChanged);
        }
        let parent_prefix = request
            .parent
            .as_ref()
            .map(|parent| format!("{}/", parent.as_str()));
        let lower = request.after.as_ref().map_or_else(
            || {
                parent_prefix
                    .as_deref()
                    .map_or(Bound::Unbounded, Bound::Included)
            },
            |after| Bound::Excluded(after.as_str()),
        );
        let candidates = state
            .paths
            .range::<str, _>((lower, Bound::Unbounded))
            .map(|(path, _)| path);
        let mut paths = candidates
            .take_while(|path| {
                parent_prefix
                    .as_ref()
                    .is_none_or(|prefix| path.as_str().starts_with(prefix))
            })
            .filter(|path| {
                request.parent.as_ref().map_or_else(
                    || !path.as_str().contains('/'),
                    |parent| path.parent().as_ref() == Some(parent),
                )
            })
            .take(request.limit as usize + 1)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = paths.len() > request.limit as usize;
        paths.truncate(request.limit as usize);
        let next_after = if has_more {
            paths.last().cloned()
        } else {
            None
        };
        Ok(ChildrenPage {
            hierarchy_version: state.hierarchy_version,
            children: paths.into_iter().map(|path| Child { path }).collect(),
            next_after,
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.commit_inner(request, None)
            .await
            .map(|(value, _)| value)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_inner(request, Some(deadline_unix_millis))
            .await
            .map(|(value, _)| value)
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.state
            .read()
            .await
            .commit(&commit_id)
            .cloned()
            .ok_or(StreamError::NotFound)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::NativeRecordStream> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            if let Err(error) = validate_limit(request.limit) {
                return Ok(Err(error));
            }
            let state = self.state.read().await;
            let path = inspect(&meter, || state.paths.get(&request.path))?;
            let Some(path) = path else {
                return Ok(Err(StreamError::NotFound));
            };
            if request.from > path.tail {
                return Ok(Err(StreamError::OutOfRange));
            }
            let count = usize::try_from((path.tail - request.from).min(u64::from(request.limit)))
                .map_err(|_| std::io::ErrorKind::InvalidInput)?;
            let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&meter);
            let mut windows = crate::native::vector(count, &meter, &mut allocation)?;
            let end = path
                .tail
                .min(request.from.saturating_add(u64::from(request.limit)));
            admit_windows(path, request.from, end, &*meter, &mut windows)?;
            windows.reverse();
            let cursor_meter = Arc::clone(&meter);
            // A bounded native read never refreshes a history root. Keep only
            // its already-admitted windows, rather than a live follow cursor.
            let cursor: VecDeque<HistoryWindow> = windows.into();
            let cursor = stream::try_unfold(cursor, move |mut cursor| {
                let meter = Arc::clone(&cursor_meter);
                async move {
                    let Some(window) = cursor.front() else {
                        return Ok(None);
                    };
                    let length = window
                        .records
                        .records
                        .get(window.start)
                        .ok_or(std::io::ErrorKind::InvalidData)?
                        .value
                        .len();
                    meter.check()?;
                    let mut bytes = NativeByteReservation::new(
                        Arc::clone(&meter),
                        NativeOperationKind::Read,
                        length,
                    )?;
                    let copied = std::mem::size_of::<Record>() as u64;
                    meter.admit_examination(1, copied)?;
                    let window = cursor.front_mut().ok_or(std::io::ErrorKind::InvalidData)?;
                    let record = window
                        .records
                        .records
                        .get(window.start)
                        .cloned()
                        .ok_or(std::io::ErrorKind::InvalidData)?;
                    window.start += 1;
                    record
                        .sequence
                        .checked_add(1)
                        .ok_or(std::io::ErrorKind::InvalidData)?;
                    if window.start == window.end {
                        cursor.pop_front();
                    }
                    #[cfg(test)]
                    let read = crate::test_meter::trace::recordings_reordered()
                        .then(|| bytes.record(length));
                    let examined = meter.record_examination(1, copied);
                    #[cfg(test)]
                    let read = read.unwrap_or_else(|| bytes.record(length));
                    #[cfg(not(test))]
                    let read = bytes.record(length);
                    examined?;
                    read?;
                    Ok(Some((
                        Ok(crate::NativeStreamOutput::new(
                            record,
                            ReadAllocation::empty(&meter),
                        )),
                        cursor,
                    )))
                }
            });
            let mut frame = acyclic_native_runtime::ReadAllocation::new(
                &meter,
                std::mem::size_of_val(&cursor),
            )?;
            allocation.absorb(&mut frame)?;
            let cursor: crate::NativeRecordStream = Box::pin(cursor);
            drop(state);
            Ok(Ok(crate::NativeStreamOutput::new(cursor, allocation)))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, u64> {
        crate::NativeProviderFuture::new(&Arc::clone(&meter), async move {
            let state = self.state.read().await;
            let value = inspect(&meter, || state.paths.get(&path).map(|path| path.tail))?;
            Ok(value
                .ok_or(StreamError::NotFound)
                .map(|value| crate::NativeStreamOutput::new(value, ReadAllocation::empty(&meter))))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, StreamBounds> {
        crate::NativeProviderFuture::new(&Arc::clone(&meter), async move {
            let state = self.state.read().await;
            let value = inspect(&meter, || {
                state
                    .paths
                    .get(&path)
                    .map(|path| StreamBounds { tail: path.tail })
            })?;
            Ok(value
                .ok_or(StreamError::NotFound)
                .map(|value| crate::NativeStreamOutput::new(value, ReadAllocation::empty(&meter))))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        crate::NativeProviderFuture::new(&Arc::clone(&meter), async move {
            let state = self.state.read().await;
            let replay = inspect(&meter, || {
                state.replay(key.as_bytes(), || meter.examine(1, 0))
            })??;
            let output = match replay {
                Some(replay) => {
                    crate::native::clone_outcome(&replay.result, &meter)?.map(|outcome| {
                        Some(IdempotencyObservation {
                            idempotency_key: key,
                            request_digest: replay.digest,
                            outcome,
                        })
                    })
                }
                None => crate::NativeStreamOutput::new(None, ReadAllocation::empty(&meter)),
            };
            meter.check()?;
            Ok(Ok(output))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome> {
        self.native_commit_guarded(request, deadline, meter, || futures::future::ready(Ok(())))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_guarded<E, F, Fut>(
        &self,
        mut request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        mut before: F,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        crate::NativeProviderFuture::new(&Arc::clone(&meter), async move {
            let work = Work::new(meter)?;
            let provider = self.with_context(Arc::clone(&work));
            let result = async {
                let _input = provider.request_claim(&request).map_err(E::from)?;
                let digest = provider.digest(&mut request).map_err(E::from)?;
                loop {
                    {
                        let state = provider.state.read().await;
                        if let Some(replay) = provider
                            .replay(&state, request.idempotency_key.as_bytes())
                            .map_err(E::from)?
                        {
                            if replay.digest != digest {
                                return Err(E::from(StreamError::IdempotencyMismatch));
                            }
                            let IdempotencyOutcome::Commit(value) = &replay.result else {
                                return Err(E::from(StreamError::IdempotencyMismatch));
                            };
                            return provider.copy_commit(value).map_err(E::from);
                        }
                    }
                    before().await?;
                    let Ok(mut state) = provider.state.try_write() else {
                        drop(provider.state.write().await);
                        continue;
                    };
                    if provider
                        .replay(&state, request.idempotency_key.as_bytes())
                        .map_err(E::from)?
                        .is_none()
                    {
                        let bytes = request
                            .mutations
                            .iter()
                            .map(|mutation| match mutation {
                                CommitMutation::Append { records, .. }
                                | CommitMutation::Fork { records, .. } => records
                                    .iter()
                                    .map(AppendRecord::payload_bytes)
                                    .sum::<usize>(),
                            })
                            .sum();
                        work.reserve_write(bytes).map_err(E::from)?;
                    }
                    return provider
                        .commit_locked(&mut state, request, digest, deadline)
                        .map_err(E::from);
                }
            }
            .await;
            work.finish(result)
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            let work = Work::new(meter)?;
            let provider = self.with_context(Arc::clone(&work));
            let result = async {
                let (plan, recovery) = provider.prepare(request, maximum_record_bytes).await?;
                work.reserve_write(maximum_record_bytes)?;
                let slot = work.record(crate::native::NativeCommitRecordSlot::new(&work.meter))?;
                let ingress = Arc::clone(&slot);
                let writer_work = Arc::clone(&work);
                let future = work.record(crate::NativeProviderFuture::try_new(
                    &work.meter,
                    async move {
                        let result = match ingress.receive().await {
                            Ok(body) => plan.settle(body).await,
                            Err(error) => Err(error),
                        };
                        writer_work.finish(result)
                    },
                ))?;
                Ok((
                    crate::PreparedNativeCommit::new(future, recovery, slot, maximum_record_bytes),
                    None,
                ))
            }
            .await;
            work.finish(result)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, AppendOutcome> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            let work = Work::new(meter)?;
            let provider = self.with_context(Arc::clone(&work));
            let existing = match &request.idempotency_key {
                Some(key) => provider.has_replay(key).await,
                None => Ok(false),
            };
            let existing = match existing {
                Ok(existing) => existing,
                Err(error) => return work.finish(Err(error)),
            };
            if !existing
                && let Err(error) = work.reserve_write(
                    request
                        .records
                        .iter()
                        .map(AppendRecord::payload_bytes)
                        .sum(),
                )
            {
                return work.finish(Err(error));
            }
            work.finish(provider.append_inner(request).await)
        })
    }
}

/// Admit the history windows that cover `from..end`, newest first.
#[cfg(not(target_arch = "wasm32"))]
fn admit_windows(
    path: &PathState,
    from: u64,
    end: u64,
    meter: &dyn NativeReadMeter,
    windows: &mut Vec<HistoryWindow>,
) -> std::io::Result<()> {
    if let Some(window) = path.prefix_window(from, end) {
        meter.admit_examination(1, 0)?;
        windows.push(window);
        return meter.record_examination(1, 0);
    }
    let mut history = HistoryWalk::new(path.history.as_ref(), end, from);
    while history.next.is_some() && history.ceiling > from {
        meter.admit_examination(1, 0)?;
        if let Some((History::Batch { records, .. }, ceiling)) = history.next()
            && let Some(window) = history_window(Arc::clone(records), ceiling, from)
        {
            windows.push(window);
        }
        meter.record_examination(1, 0)?;
    }
    Ok(())
}
