#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StreamProvider for MemoryStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let state = self.state.read().await;
        Ok(state
            .replay(idempotency_key.as_bytes())
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
        self.append_inner(request)
            .await
            .map(|(value, _claim)| value)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        if request.source == request.destination {
            return Err(StreamError::InvalidArgument);
        }
        validate_fork_size(&request)?;
        let digest = fork_digest(&request)?;
        let mut state = self.state.write().await;
        if let Some(result) = replay_fork(&state, request.idempotency_key.as_ref(), digest)? {
            return Ok(result);
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        let forked_at = crate::preparation::fork_cut(
            &request,
            state.paths.get(&request.source).map(|stream| stream.tail),
            state
                .paths
                .get(&request.destination)
                .map(|stream| stream.tail),
        )?;
        let source = state
            .paths
            .get(&request.source)
            .ok_or(StreamError::NotFound)?;
        let source_history = source.history.clone();
        reserve_paths(&state, &request.destination, self.limits)?;
        reserve_commit(&state, self.limits)?;
        let (paths, _claim) = self.paths(&state, std::iter::once(&request.destination))?;
        let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
        let commit_id = commit_identity(decision, digest);
        let history = Arc::new(History::Prefix {
            source: source_history,
            tail: forked_at,
            _allocation: Default::default(),
        });
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
        publish(&mut state, paths, vec![history], &envelope);
        state.decision = decision;
        retain(
            &mut state,
            Some(envelope),
            request.idempotency_key,
            digest,
            IdempotencyOutcome::Fork(receipt.clone()),
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
        let records = read_history(
            path.history.as_ref(),
            path.tail,
            request.from,
            usize::try_from(request.limit).map_err(|_| StreamError::LimitExceeded)?,
        );
        Ok(stream::iter(records.into_iter().map(Ok)).boxed())
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let (receiver, cursor) = {
            let state = self.state.read().await;
            let stream = state.paths.get(&path).ok_or(StreamError::NotFound)?;
            if from > stream.tail {
                return Err(StreamError::OutOfRange);
            }
            (
                stream.changed.subscribe(),
                HistoryCursor::new(stream.history.as_ref(), stream.tail, from),
            )
        };
        let provider = self.clone();
        Ok(stream::unfold(
            (provider, path, receiver, cursor),
            |(provider, path, mut receiver, mut cursor)| async move {
                loop {
                    let record = provider.next_follow_record(&path, &mut cursor).await;
                    let Some(record) = (match record {
                        Ok(record) => record,
                        Err(error) => {
                            return Some((Err(error), (provider, path, receiver, cursor)));
                        }
                    }) else {
                        if receiver.changed().await.is_err() {
                            return Some((
                                Err(StreamError::Unavailable),
                                (provider, path, receiver, cursor),
                            ));
                        }
                        continue;
                    };
                    return Some((Ok(record), (provider, path, receiver, cursor)));
                }
            },
        )
        .boxed())
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        let page = self
            .children_page(ChildrenPageRequest {
                parent: request.parent,
                limit: request.limit,
                after: None,
                hierarchy_version: None,
            })
            .await?;
        Ok(stream::iter(page.children.into_iter().map(Ok)).boxed())
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
                |parent| is_direct_child(parent, after),
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
                    |parent| is_direct_child(parent, path),
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
            .map(|(value, _claim)| value)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_inner(request, Some(deadline_unix_millis))
            .await
            .map(|(value, _claim)| value)
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
            let mut operation = acyclic_native_runtime::NativeOperationReservation::new(
                Arc::clone(&meter),
                acyclic_native_runtime::NativeOperationKind::Read,
                1,
            )?;
            let state = self.state.read().await;
            meter.admit_examination(1, 0)?;
            let (path, recorded) = operation.attempt(|| state.paths.get(&request.path));
            let examined = meter.record_examination(1, 0);
            recorded?;
            examined?;
            let Some(path) = path else {
                return Ok(Err(StreamError::NotFound));
            };
            if request.from > path.tail {
                return Ok(Err(StreamError::OutOfRange));
            }
            let mut count = 0usize;
            let mut history = HistoryWalk::new(path.history.as_ref(), path.tail, request.from);
            while history.next.is_some() && history.ceiling > request.from {
                meter.admit_examination(1, 0)?;
                if let Some((History::Batch { .. }, _)) = history.next() {
                    count = count
                        .checked_add(1)
                        .ok_or(std::io::ErrorKind::OutOfMemory)?;
                }
                meter.record_examination(1, 0)?;
            }
            let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&meter);
            let mut windows = crate::native::vector(count, &meter, &mut allocation)?;
            let mut history = HistoryWalk::new(path.history.as_ref(), path.tail, request.from);
            while history.next.is_some() && history.ceiling > request.from {
                meter.admit_examination(1, 0)?;
                if let Some((History::Batch { records, .. }, ceiling)) = history.next()
                    && let Some(window) = history_window(Arc::clone(records), ceiling, request.from)
                {
                    windows.push(window);
                }
                meter.record_examination(1, 0)?;
            }
            windows.reverse();
            let cursor_meter = Arc::clone(&meter);
            let records = windows
                .into_iter()
                .flat_map(|window| {
                    (window.start..window.end)
                        .map(move |index| (Arc::clone(&window.records), index))
                })
                .take(request.limit as usize);
            let cursor = stream::iter(records).scan(false, move |done, (records, index)| {
                let result = if *done {
                    None
                } else {
                    let result = (|| {
                        cursor_meter.check()?;
                        let source = &records[index];
                        let mut bytes = acyclic_native_runtime::NativeByteReservation::new(
                            Arc::clone(&cursor_meter),
                            acyclic_native_runtime::NativeOperationKind::Read,
                            source.value.len(),
                        )?;
                        let copied = std::mem::size_of::<Record>() as u64;
                        cursor_meter.admit_examination(1, copied)?;
                        let record = source.clone();
                        let examined = cursor_meter.record_examination(1, copied);
                        let read = bytes.record(source.value.len());
                        examined?;
                        read?;
                        Ok(Ok(crate::NativeStreamOutput::new(
                            record,
                            acyclic_native_runtime::ReadAllocation::empty(&cursor_meter),
                        )))
                    })();
                    *done = result.is_err();
                    Some(result)
                };
                futures::future::ready(result)
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
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, u64> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            let mut operation = acyclic_native_runtime::NativeOperationReservation::new(
                Arc::clone(&meter),
                acyclic_native_runtime::NativeOperationKind::Read,
                1,
            )?;
            let state = self.state.read().await;
            meter.admit_examination(1, 0)?;
            let (result, recorded) = operation.attempt(|| {
                state
                    .paths
                    .get(&path)
                    .map(|path| {
                        crate::NativeStreamOutput::new(
                            path.tail,
                            acyclic_native_runtime::ReadAllocation::empty(&meter),
                        )
                    })
                    .ok_or(StreamError::NotFound)
            });
            let examined = meter.record_examination(1, 0);
            recorded?;
            examined?;
            Ok(result)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, StreamBounds> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            let mut operation = acyclic_native_runtime::NativeOperationReservation::new(
                Arc::clone(&meter),
                acyclic_native_runtime::NativeOperationKind::Read,
                1,
            )?;
            let state = self.state.read().await;
            meter.admit_examination(1, 0)?;
            let (result, recorded) = operation.attempt(|| {
                state
                    .paths
                    .get(&path)
                    .map(|path| {
                        crate::NativeStreamOutput::new(
                            StreamBounds { tail: path.tail },
                            acyclic_native_runtime::ReadAllocation::empty(&meter),
                        )
                    })
                    .ok_or(StreamError::NotFound)
            });
            let examined = meter.record_examination(1, 0);
            recorded?;
            examined?;
            Ok(result)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::new(&original, async move {
            let mut operation = acyclic_native_runtime::NativeOperationReservation::new(
                Arc::clone(&meter),
                acyclic_native_runtime::NativeOperationKind::Read,
                1,
            )?;
            let state = self.state.read().await;
            meter.admit_examination(1, 0)?;
            let (replay, recorded) = operation.attempt(|| state.replay(key.as_bytes()));
            let examined = meter.record_examination(1, 0);
            recorded?;
            examined?;
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
                None => crate::NativeStreamOutput::new(
                    None,
                    acyclic_native_runtime::ReadAllocation::empty(&meter),
                ),
            };
            meter.check()?;
            Ok(Ok(output))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome> {
        self.native_commit_guarded(request, None, meter, || futures::future::ready(Ok(())))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome> {
        self.native_commit_guarded(request, Some(deadline), meter, || {
            futures::future::ready(Ok(()))
        })
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
                normalize_commit(&mut request).map_err(E::from)?;
                validate_commit_shape(&request).map_err(E::from)?;
                let digest = provider.digest(&mut request).map_err(E::from)?;
                loop {
                    {
                        let state = provider.state.read().await;
                        if let Some(replay) = state.replay(request.idempotency_key.as_bytes()) {
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
                    let mut state = match provider.state.try_write() {
                        Ok(state) => state,
                        Err(_) => {
                            drop(provider.state.write().await);
                            continue;
                        }
                    };
                    if !state
                        .replays
                        .contains_key(request.idempotency_key.as_bytes())
                    {
                        let bytes = request
                            .mutations
                            .iter()
                            .map(|mutation| match mutation {
                                CommitMutation::Append { records, .. }
                                | CommitMutation::Fork { records, .. } => {
                                    records.iter().map(Bytes::len).sum::<usize>()
                                }
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
            let provider = Self {
                work: Some(Arc::clone(&work)),
                ..self.clone()
            };
            let (plan, recovery) = match provider.prepare(request, maximum_record_bytes).await {
                Ok(prepared) => prepared,
                Err(error) => return work.finish(Err(error)),
            };
            if let Err(error) = work.reserve_write(maximum_record_bytes) {
                return work.finish(Err(error));
            }
            let slot = crate::native::NativeCommitRecordSlot::new(&work.meter)?;
            let ingress = Arc::clone(&slot);
            let writer_work = Arc::clone(&work);
            let future = crate::NativeProviderFuture::try_new(&work.meter, async move {
                let result = match ingress.receive().await {
                    Ok(body) => plan.settle(body).await,
                    Err(error) => Err(error),
                };
                writer_work.finish(result)
            })?;
            work.finish(Ok((
                crate::PreparedNativeCommit::new(future, recovery, slot, maximum_record_bytes),
                None,
            )))
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
            let provider = Self {
                work: Some(Arc::clone(&work)),
                ..self.clone()
            };
            let existing = request.idempotency_key.as_ref().is_some_and(|key| {
                provider
                    .state
                    .try_read()
                    .is_ok_and(|state| state.replays.contains_key(key.as_bytes()))
            });
            if !existing {
                if let Err(error) = work.reserve_write(request.records.iter().map(Bytes::len).sum())
                {
                    return work.finish(Err(error));
                }
            }
            work.finish(provider.append_inner(request).await)
        })
    }
}
