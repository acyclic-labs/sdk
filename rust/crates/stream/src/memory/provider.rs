impl MemoryStream {
    /// Captures an already validated request whose minimum cut was awaited
    /// outside any adapter publication lock. There is no boxed inner dispatch.
    pub(crate) async fn capture_heads(
        &self,
        request: &crate::HeadsRequest,
    ) -> Result<crate::Heads, StreamError> {
        let state = self.state.read().await;
        let retained = request
            .retained
            .iter()
            .map(|selected| crate::RetainedRecordObservation {
                path: selected.path.clone(),
                record: (|| {
                    let envelope = state.commit(&selected.commit_id)?;
                    let index = envelope
                        .mutations
                        .binary_search_by(|mutation| {
                            let destination = match mutation {
                                CommittedMutation::Append(append) => &append.path,
                                CommittedMutation::Fork(fork) => &fork.destination,
                            };
                            destination.cmp(&selected.path)
                        })
                        .ok()?;
                    let (start, records) = match &envelope.mutations[index] {
                        CommittedMutation::Append(append) => (append.start, &append.records),
                        CommittedMutation::Fork(fork) => (fork.forked_at, &fork.records),
                    };
                    let offset = usize::try_from(selected.sequence.checked_sub(start)?).ok()?;
                    records.get(offset).cloned()
                })(),
            })
            .collect();
        let heads = request
            .paths
            .iter()
            .map(|path| head_at(&state, path, request.projection, request.durable_through))
            .collect::<Result<_, _>>()?;
        let mut children = request.children.clone();
        if let Some(children) = &mut children {
            children.limit = children
                .limit
                .min(request.limit - request.selection_count()?);
        }
        let view = crate::Heads {
            retained,
            projection: request.projection,
            snapshot: self.revisions.at(state.decision),
            heads,
            children: children
                .map(|request| children_at(&state, request))
                .transpose()?,
        };
        if !request.at_least.proven_by(Some(&view.snapshot)) {
            return Err(StreamError::InvalidResponse);
        }
        Ok(view)
    }
}

fn head_at(
    state: &State,
    path: &StreamPath,
    projection: crate::HeadProjection,
    through: Option<u64>,
) -> Result<Option<crate::PathHead>, StreamError> {
    state
        .paths
        .get(path)
        .map(|stream| {
            Ok(crate::PathHead {
                retained_through: through.filter(|through| *through <= stream.tail),
                tail: stream.tail,
                last: (projection == crate::HeadProjection::History)
                    .then(|| last_record(stream))
                    .flatten(),
                durable: 0,
                origin: if projection == crate::HeadProjection::History {
                    state.paths.origin(path)?
                } else {
                    None
                },
            })
        })
        .transpose()
}

fn namespace_page(
    index: &Index,
    lower: Option<&str>,
    parent: Option<&StreamPath>,
    limit: usize,
    out: &mut Vec<StreamPath>,
) -> Result<(), StreamError> {
    let Some(edge) = index else {
        return Ok(());
    };
    if lower.is_none_or(|after| edge.name.text.as_str() > after) {
        namespace_page(&edge.left, lower, parent, limit, out)?;
        if out.len() == limit {
            return Ok(());
        }
        out.push(StreamPath::new(parent.map_or_else(
            || edge.name.text.to_string(),
            |parent| format!("{}/{}", parent.as_str(), edge.name.text),
        ))?);
    }
    if out.len() < limit {
        namespace_page(&edge.right, lower, parent, limit, out)?;
    }
    Ok(())
}

fn projected_tail(
    paths: &Namespace,
    path: &StreamPath,
    relative: Option<&StreamPath>,
) -> Result<Option<Record>, StreamError> {
    let Some(relative) = relative else {
        return Ok(None);
    };
    // A combined path over the bound cannot exist, so that child has no tail;
    // one long child never fails the page.
    if path.as_str().len() + 1 + relative.as_str().len() > crate::MAX_PATH_BYTES {
        return Ok(None);
    }
    let target = StreamPath::new(format!("{}/{}", path.as_str(), relative.as_str()))?;
    let Some(stream) = paths.get(&target) else {
        return Ok(None);
    };
    Ok(last_record(stream))
}

fn last_record(stream: &PathState) -> Option<Record> {
    let Some(from) = stream.tail.checked_sub(1) else {
        return None;
    };
    if let Some(window) = stream.prefix_window(from, stream.tail) {
        return window.records.records.get(window.start).cloned();
    }
    HistoryWalk::new(stream.history.as_ref(), stream.tail, from).find_map(|(history, ceiling)| {
        if let History::Batch { records, .. } = history {
            history_window(Arc::clone(records), ceiling, from)
                .and_then(|window| window.records.records.get(window.start).cloned())
        } else {
            None
        }
    })
}

fn children_at(state: &State, request: ChildrenPageRequest) -> Result<ChildrenPage, StreamError> {
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
    if request
        .hierarchy_version
        .is_some_and(|version| version != state.hierarchy_version)
    {
        return Err(StreamError::HierarchyChanged);
    }
    let children = request
        .parent
        .as_ref()
        .map_or(Some(&state.paths.roots), |parent| {
            state.paths.node(parent).map(|node| &node.children)
        });
    let lower = request
        .after
        .as_ref()
        .map(|path| path.as_str().rsplit('/').next().unwrap_or_default());
    let mut paths = Vec::new();
    if let Some(children) = children {
        namespace_page(
            children,
            lower,
            request.parent.as_ref(),
            request.limit as usize + 1,
            &mut paths,
        )?;
    }
    let has_more = paths.len() > request.limit as usize;
    paths.truncate(request.limit as usize);
    let next_after = if has_more {
        paths.last().cloned()
    } else {
        None
    };
    let mut page_bytes =
        64usize.saturating_add(next_after.as_ref().map_or(0, |path| path.as_str().len()));
    let children = paths
        .into_iter()
        .map(|path| {
            let tail_record = projected_tail(&state.paths, &path, request.tail_stream.as_ref())?;
            let origin = state.paths.origin(&path)?;
            let child = Child {
                path,
                tail_record,
                origin,
            };
            #[cfg(all(loom, feature = "local"))]
            let size =
                if crate::verification::fault_is(crate::verification::Fault::IgnorePageRecordBytes)
                {
                    child.path.as_str().len()
                } else {
                    crate::wire_codec::requests::child_len(&child)
                };
            #[cfg(not(all(loom, feature = "local")))]
            let size = crate::wire_codec::requests::child_len(&child);
            page_bytes = page_bytes.saturating_add(size).saturating_add(10);
            if page_bytes > crate::MAX_COMMAND_BYTES {
                return Err(StreamError::LimitExceeded);
            }
            Ok(child)
        })
        .collect::<Result<Vec<_>, StreamError>>()?;
    let result = ChildrenPage {
        hierarchy_version: state.hierarchy_version,
        children,
        next_after,
    };
    Ok(result)
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StreamProvider for MemoryStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.operation_claims).with_process_exclusion()
    }

    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        self.revisions.wait(&request.at_least).await?;
        let view = self.capture_heads(&request).await?;
        view.check_size()?;
        Ok(view)
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
                revision: Some(self.revisions.at(replay.index)),
            }))
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        match request {
            crate::WriteRequest::Append(request) => Box::pin(async move {
                self.append_inner(request)
                    .await
                    .map(|(outcome, _)| crate::WriteOutcome::Append(outcome))
            }),
            crate::WriteRequest::Fork(request) => Box::pin(async move {
                let mut state = self.state.write().await;
                self.plan_fork_locked(&mut state, &request)?
                    .install(self, &mut state)
                    .map(crate::WriteOutcome::Fork)
            }),
            crate::WriteRequest::Commit(request) => Box::pin(async move {
                self.commit_inner(request, None)
                    .await
                    .map(|(outcome, _)| crate::WriteOutcome::Commit(outcome))
            }),
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        validate_limit(request.limit)?;
        if let Some(key) = &request.key {
            key.validate()?;
        }
        self.revisions.wait(&request.at_least).await?;
        let state = self.state.read().await;
        let path = state
            .paths
            .get(&request.path)
            .ok_or(StreamError::NotFound)?;
        if request.from > path.tail {
            return Err(StreamError::OutOfRange);
        }
        let cursor = FiniteRead::new(path, request);
        drop(state);
        Ok(stream::try_unfold(cursor, |mut cursor| async move {
            if cursor
                .peek(|| Ok::<_, StreamError>(()), || StreamError::Unavailable)?
                .is_none()
            {
                return Ok(None);
            }
            Ok(cursor.take().map(|record| (record, cursor)))
        })
        .boxed())
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
        self.revisions.wait(&at_least).await?;
        if progress {
            self.progress_from(path, from, preceding, when_absent, filter)
                .await
        } else {
            self.follow_from(path, from, preceding, when_absent, filter)
                .await
        }
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.state
            .read()
            .await
            .find_commit(&commit_id, || {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = &self.work {
                    work.record(work.meter.examine(1, 0))?;
                }
                Ok::<_, StreamError>(())
            })?
            .cloned()
            .ok_or(StreamError::NotFound)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::NativeStreamProvider for MemoryStream {
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
            if let Some(key) = &request.key
                && let Err(error) = key.validate()
            {
                return Ok(Err(error));
            }
            if let Err(error) = self.revisions.wait(&request.at_least).await {
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
            // Only the immutable pin and one inline page survive opening.
            // History traversal and key selection belong to cursor pulls.
            meter.admit_examination(1, 0)?;
            let cursor = FiniteRead::new(path, request);
            meter.record_examination(1, 0)?;
            drop(state);
            let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&meter);
            let cursor_meter = Arc::clone(&meter);
            let cursor = stream::try_unfold(cursor, move |mut cursor| {
                let meter = Arc::clone(&cursor_meter);
                async move {
                    let Some(record) = cursor.peek(
                        || meter.examine(1, 0),
                        || std::io::ErrorKind::InvalidData.into(),
                    )?
                    else {
                        return Ok(None);
                    };
                    let length = record.value.len();
                    meter.check()?;
                    let mut bytes = acyclic_native_runtime::NativeByteReservation::new(
                        Arc::clone(&meter),
                        NativeOperationKind::Read,
                        length,
                    )?;
                    let copied = std::mem::size_of::<Record>() as u64;
                    meter.admit_examination(1, copied)?;
                    let record = cursor.take().ok_or(std::io::ErrorKind::InvalidData)?;
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
                            revision: Some(self.revisions.at(replay.index)),
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
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: F,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        self.guarded_commit(request, deadline, meter, before, std::convert::identity)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        Work::start(meter, |work| async move {
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
    fn native_write(
        &self,
        request: crate::WriteRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::WriteOutcome> {
        match request {
            crate::WriteRequest::Append(request) => {
                Work::start(meter, |work| async move {
                    let provider = self.with_context(Arc::clone(&work));
                    let existing = provider.has_replay(&request.idempotency_key).await;
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
                    work.finish(provider.append_inner(request).await.map(
                        |(outcome, allocation)| (crate::WriteOutcome::Append(outcome), allocation),
                    ))
                })
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
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryStream {
    fn guarded_commit<T, C, E, F, Fut>(
        &self,
        mut request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        mut before: F,
        convert: C,
    ) -> crate::NativeProviderFuture<'_, T, E>
    where
        T: Send + 'static,
        C: FnOnce(CommitOutcome) -> T + Send + 'static,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        Work::start(meter, |work| async move {
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
                            return provider
                                .receipt(value, provider.revisions.at(replay.index))
                                .map(|(receipt, claim)| (CommitOutcome::Committed(receipt), claim))
                                .map_err(E::from);
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
            work.finish(result.map(|(outcome, allocation)| (convert(outcome), allocation)))
        })
    }
}
