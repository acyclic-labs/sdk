use super::*;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StreamProvider for MemoryStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        ordinary(self.inspect_inner(idempotency_key, None).await)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        ordinary(self.bounds_inner(path, None).await).map(|bounds| bounds.tail)
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        ordinary(self.bounds_inner(path, None).await)
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let completion = self.apply_append(&request, None, None).await;
        ordinary(completion.map(|(outcome, error)| {
            assert!(error.is_none(), "ordinary appends have no fallible meter");
            outcome
        }))
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        if request.source == request.destination {
            return Err(StreamError::InvalidArgument);
        }
        validate_fork_size(&request)?;
        let digest = fork_digest(&request);
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
        let missing = provider_result(missing_paths(
            &state,
            std::iter::once(&request.destination),
            None,
        ))?;
        reserve(&state, &missing.value, 0, 0, self.limits)?;
        provider_result(prepare_retained_capacity(&mut state, None))?;
        let decision = state
            .commits
            .len()
            .checked_add(1)
            .and_then(|count| u64::try_from(count).ok())
            .ok_or(StreamError::Capacity)?;
        let commit_id = provider_result(
            planned_commit_id(decision, digest, None).map_err(MemoryFailure::from),
        )?;
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Fork(CommittedFork {
                source: request.source.clone(),
                destination: request.destination.clone(),
                forked_at,
                tail: forked_at,
                records: Vec::new(),
            })],
        };
        let paths = provider_result(prepare_paths(&missing.value, &envelope, None))?;
        let mutations = provider_result(prepare_mutations(&state, &envelope, None))?;
        drop(missing);
        let receipt = ForkReceipt {
            source: request.source,
            destination: request.destination,
            forked_at,
            tail: forked_at,
            commit_id,
        };
        let cached = provider_result(
            cache_replay(
                None,
                Some(envelope),
                IdempotencyOutcome::Fork(receipt.clone()),
                Allocation::default(),
            )
            .map_err(MemoryFailure::Observer),
        )?;
        publish(
            &mut state,
            PreparedPublication {
                cached,
                key: request.idempotency_key.map(|key| key.0),
                paths,
                mutations,
                resident: Allocation::default(),
            },
            digest,
        );
        Ok(receipt)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        validate_limit(request.limit)?;
        let records = provider_result(self.read_inner(request, None).await)?;
        Ok(stream::iter(record_iterator(records))
            .map(|record| Ok(record.expect("ordinary reads have no fallible meter")))
            .boxed())
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
        let prefix = request
            .parent
            .as_ref()
            .map(|parent| format!("{}/", parent.as_str()));
        let lower = request.after.as_ref().map_or_else(
            || prefix.as_deref().map_or(Bound::Unbounded, Bound::Included),
            |after| Bound::Excluded(after.as_str()),
        );
        let mut paths = state
            .paths
            .range::<str, _>((lower, Bound::Unbounded))
            .map(|(path, _)| path)
            .take_while(|path| {
                prefix
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
        self.commit_value(request, None, None).await
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_value(request, Some(deadline_unix_millis), None)
            .await
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.state
            .read()
            .await
            .commits
            .get(&commit_id)
            .and_then(|cached| cached.envelope())
            .cloned()
            .ok_or(StreamError::NotFound)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            native(self.inspect_inner(key, Some(&meter)).await, &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            native(
                self.bounds_inner(path, Some(&meter))
                    .await
                    .map(|output| output.map(|bounds| bounds.tail)),
                &meter,
            )
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, StreamBounds> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            native(self.bounds_inner(path, Some(&meter)).await, &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            if let Err(error) = validate_limit(request.limit) {
                return Ok(Err(error));
            }
            let cursor_meter = Arc::clone(&meter);
            let cursor = stream::iter(record_iterator((NativeStreamOutput::default(), None))).map(
                move |record| {
                    record.map(|record| {
                        Ok(NativeStreamOutput::new(
                            record,
                            ReadAllocation::empty(&cursor_meter),
                        ))
                    })
                },
            );
            let allocation = allocate(Some(&meter), std::mem::size_of_val(&cursor))?;
            let mut cursor = NativeStreamOutput {
                value: Box::pin(cursor),
                allocation,
            };
            let result = self.read_inner(request, Some(&meter)).await.map(|records| {
                *cursor.value.as_mut().get_mut().get_mut().get_mut() = record_iterator(records);
                NativeStreamOutput {
                    value: cursor.value as NativeRecordStream,
                    allocation: cursor.allocation,
                }
            });
            native(result, &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            match self.apply_append(&request, Some(&meter), None).await {
                Ok((outcome, error)) => match error {
                    Some(error) => Err(error),
                    None => native(Ok(outcome), &meter),
                },
                Err(error) => native(Err(error), &meter),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.commit_future(request, None, meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
        mut before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let mut input = crate::capture_commit(&request, &meter)?;
            drop(request);
            match self
                .apply_commit(
                    &mut input.value,
                    deadline_unix_millis,
                    Some(&meter),
                    Some(&mut before),
                    None,
                )
                .await
            {
                Ok(Ok((outcome, error, _))) => match error {
                    Some(error) => Err(error),
                    None => native(Ok(outcome), &meter).map(|result| result.map_err(E::from)),
                },
                Ok(Err(error)) => Ok(Err(error)),
                Err(error) => native(Err(error), &meter).map(|result| result.map_err(E::from)),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let mut input = crate::capture_commit(&request, &meter)?;
            drop(request);
            let prepared = match self
                .prepare_commit(&mut input.value, maximum_record_bytes, Arc::clone(&meter))
                .await?
            {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };
            let recovery = match self.prepare_recovery(&prepared) {
                Ok(future) => future,
                Err(MemoryFailure::Observer(error)) => return Err(error),
                Err(MemoryFailure::Provider(error)) => return Ok(Err(error)),
            };
            let slot = crate::NativeCommitRecordSlot::new(&meter)?;
            let received = Arc::clone(&slot);
            let provider = self.clone();
            let finish_meter = Arc::clone(&meter);
            // Fund the completion future before accepting the final record.
            let future = NativeProviderFuture::try_new(&meter, async move {
                let body = match received.receive().await {
                    Ok(body) => body,
                    Err(error) => return Ok(Err(error)),
                };
                let [CommitMutation::Append { records, .. }] = input.value.mutations.as_mut_slice()
                else {
                    unreachable!("the private template was validated before issuance")
                };
                records[0] = body;
                match provider.finish_commit(
                    &mut *provider.state.write().await,
                    prepared,
                    &mut input.value,
                    None,
                )? {
                    Ok((outcome, error)) => match error {
                        Some(error) => Err(error),
                        None => native(Ok(outcome), &finish_meter),
                    },
                    Err(error) => Ok(Err(error)),
                }
            })?;
            Ok(Ok(NativeStreamOutput::new(
                crate::PreparedNativeCommit::new(future, recovery, slot, maximum_record_bytes),
                ReadAllocation::empty(&meter),
            )))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.commit_future(request, Some(deadline_unix_millis), meter)
    }
}
