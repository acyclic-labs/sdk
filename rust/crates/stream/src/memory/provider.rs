#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum HeadDurability {
    Volatile,
    Durable,
    Deferred,
}

impl MemoryStream {
    pub(crate) async fn follow_inner(
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
            bootstrap_head,
            until,
        } = request;
        self.revisions.wait(&at_least).await?;
        if progress {
            self.progress_from(
                path,
                from,
                preceding,
                when_absent,
                filter,
                bootstrap_head,
                until,
            )
            .await
        } else {
            self.follow_from(
                path,
                from,
                preceding,
                when_absent,
                filter,
                bootstrap_head,
                until,
            )
            .await
        }
    }

    pub(crate) async fn heads_inner(
        &self,
        request: crate::HeadsRequest,
    ) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        self.revisions.wait(&request.at_least).await?;
        let view = self
            .capture_heads(&request, HeadDurability::Volatile)
            .await?;
        Ok(view)
    }

    pub(crate) async fn inspect_idempotency_inner(
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

    pub(crate) async fn read_inner(
        &self,
        request: crate::ValidatedReadRequest,
    ) -> Result<RecordStream, StreamError> {
        self.revisions.wait(&request.request().at_least).await?;
        let request = request.into_request();
        let state = self.state.read().await;
        let path = state
            .paths
            .get(&request.path)
            .ok_or(StreamError::NotFound)?;
        if request.from > path.tail {
            return Err(StreamError::OutOfRange);
        }
        let cursor = FiniteRead::new(path, request);
        let snapshot = self.revisions.at(state.decision);
        let selection = cursor.selection();
        drop(state);
        Ok(crate::Opened::new(
            snapshot,
            selection,
            stream::iter(cursor).boxed(),
        ))
    }

    pub(crate) async fn read_commit_inner(
        &self,
        commit_id: CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.state
            .read()
            .await
            .find_commit(&commit_id, |items| {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = &self.work {
                    work.record(work.meter.examine(items, 0))?;
                }
                let _ = items;
                Ok::<_, StreamError>(())
            })?
            .cloned()
            .ok_or(StreamError::NotFound)
    }
}

impl MemoryStream {
    /// Captures an already validated request whose minimum cut was awaited
    /// outside any adapter publication lock. There is no boxed inner dispatch.
    /// Durable providers confirm the captured state. Deferred providers reserve
    /// later watermark fields without claiming durability during capture.
    pub(crate) async fn capture_heads(
        &self,
        request: &crate::HeadsRequest,
        durability: HeadDurability,
    ) -> Result<crate::Heads, StreamError> {
        let mut allocation = None;
        let mut view = self.empty_heads(request);
        view.heads
            .try_reserve_exact(request.paths.len())
            .map_err(|_| StreamError::Capacity)?;
        self.capture_heads_into(request, durability, &mut view, None, &mut allocation)
            .await?;
        Ok(view)
    }

    fn empty_heads(&self, request: &crate::HeadsRequest) -> crate::Heads {
        crate::Heads {
            applied: self.revisions.at(0),
            retained: Default::default(),
            projection: request.projection,
            snapshot: self.revisions.at(0),
            heads: Default::default(),
            children: None,
        }
    }

    // The native caller admits any spill before entering this same capture.
    async fn capture_heads_into(
        &self,
        request: &crate::HeadsRequest,
        durability: HeadDurability,
        view: &mut crate::Heads,
        _work: CaptureWork<'_>,
        allocation: &mut Allocation,
    ) -> Result<(), StreamError> {
        let state = self.state.read().await;
        let applied = self.revisions.at(state.decision);
        let index = if let Some(at) = &request.at {
            if !at.same_authority(&applied) {
                return Err(StreamError::RevisionForeign);
            }
            if at.index > state.decision {
                return Err(StreamError::Unavailable);
            }
            at.index
        } else {
            state.decision
        };
        let empty = Namespace::default();
        let (paths, hierarchy) = if index == state.decision {
            (&state.paths, state.hierarchy_version)
        } else if index == 0 {
            (&empty, CommitId::from_bytes([0; 32]))
        } else {
            let cut = state
                .namespace_at(index, || {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = _work {
                        return work.record(work.meter.examine(1, 0));
                    }
                    Ok::<_, StreamError>(())
                })?
                .ok_or(StreamError::PrefixNotRetained)?;
            (&cut.paths, cut.hierarchy_version)
        };
        view.retained = request
            .retained
            .iter()
            .map(|selected| crate::RetainedRecordObservation {
                path: selected.path.clone(),
                record: (|| {
                    let fact = state
                        .find_commit_fact(&selected.commit_id, |_| {
                            Ok::<_, std::convert::Infallible>(())
                        })
                        .ok()??;
                    if fact.index > index {
                        return None;
                    }
                    let envelope = fact.envelope()?;
                    let index = envelope
                        .mutations
                        .binary_search_by(|mutation| {
                            crate::request::fact_path(mutation).cmp(&selected.path)
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
        for path in &request.paths {
            let capture = || {
                head_at(
                    paths,
                    path,
                    request.projection,
                    request.durable_through,
                    durability == HeadDurability::Durable,
                    _work,
                )
            };
            #[cfg(not(target_arch = "wasm32"))]
            let head = if let Some(work) = _work {
                work.record(inspect(&work.meter, capture))??
            } else {
                capture()?
            };
            #[cfg(target_arch = "wasm32")]
            let head = capture()?;
            view.heads.push(head);
        }
        view.snapshot = self.revisions.at(index);
        view.applied = applied;
        let deferred = durability == HeadDurability::Deferred
            && request.projection != crate::HeadProjection::Tail;
        let remaining = crate::MAX_COMMAND_BYTES.saturating_sub(
            crate::wire_codec::requests::heads_len_with_durability(view, deferred),
        );
        view.children = if let Some(children) = &request.children {
            let limit = children
                .limit
                .min(request.limit - request.selection_count()?);
            Some(children_at(
                paths, hierarchy, children, limit, remaining, _work, allocation,
            )?)
        } else {
            None
        };
        if !request.at_least.proven_by(Some(&view.applied)) {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}

fn head_at(
    paths: &Namespace,
    path: &StreamPath,
    projection: crate::HeadProjection,
    through: Option<u64>,
    durable: bool,
    _work: CaptureWork<'_>,
) -> Result<Option<crate::PathHead>, StreamError> {
    paths
        .observation_with(path, || {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = _work {
                return work.record(work.meter.examine(1, 0));
            }
            Ok(())
        })?
        .map(|observation| {
            let stream = &observation.node.data;
            Ok(crate::PathHead {
                changed_at: observation.changed_at(),
                retained_through: through.filter(|through| *through <= stream.tail),
                tail: stream.tail,
                last: if projection == crate::HeadProjection::History {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = _work {
                        native_last_record(stream, work)?
                    } else {
                        last_record(stream)
                    }
                    #[cfg(target_arch = "wasm32")]
                    last_record(stream)
                } else {
                    None
                },
                durable: if durable && projection != crate::HeadProjection::Tail {
                    stream.tail
                } else {
                    0
                },
                origin: if projection == crate::HeadProjection::History {
                    capture_origin(paths, path, _work)?
                } else {
                    None
                },
            })
        })
        .transpose()
}

fn capture_origin(
    paths: &Namespace,
    path: &StreamPath,
    _work: CaptureWork<'_>,
) -> Result<Option<crate::Provenance<StreamPath, crate::ForkCut>>, StreamError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = _work {
        return paths.origin_with(
            path,
            || work.record(work.meter.examine(1, 0)),
            |source, relative| work.record(source.with_native_suffix(relative, &work.meter))?,
        );
    }
    paths.origin(path)
}

struct NamespacePage<'a> {
    index: &'a Index,
    start: u64,
    length: usize,
    has_more: bool,
}

fn namespace_page<'a>(
    index: &'a Index,
    lower: Option<&str>,
    offset: u64,
    limit: usize,
    examine: &mut impl FnMut(&str) -> Result<(), StreamError>,
) -> Result<NamespacePage<'a>, StreamError> {
    let mut start = 0u64;
    if let Some(lower) = lower {
        let mut cursor = index;
        while let Some(edge) = cursor {
            examine(&edge.name.text)?;
            if edge.name.text.as_str() <= lower {
                start += width(&edge.left) as u64 + 1;
                cursor = &edge.right;
            } else {
                cursor = &edge.left;
            }
        }
    }
    let start = start.saturating_add(offset);
    let available = (width(index) as u64).saturating_sub(start);
    Ok(NamespacePage {
        index,
        start,
        length: available.min(limit as u64) as usize,
        has_more: available > limit as u64,
    })
}
impl NamespacePage<'_> {
    fn visit(
        &self,
        examine: &mut impl FnMut(&str) -> Result<(), StreamError>,
        emit: &mut impl FnMut(&str) -> Result<bool, StreamError>,
    ) -> Result<(), StreamError> {
        let mut skip = self.start;
        let mut remaining = self.length;
        namespace_rank_page(self.index, &mut skip, &mut remaining, examine, emit)
    }
}
fn namespace_rank_page(
    index: &Index,
    skip: &mut u64,
    remaining: &mut usize,
    examine: &mut impl FnMut(&str) -> Result<(), StreamError>,
    emit: &mut impl FnMut(&str) -> Result<bool, StreamError>,
) -> Result<(), StreamError> {
    let Some(edge) = index else {
        return Ok(());
    };
    if *remaining == 0 {
        return Ok(());
    }
    examine(&edge.name.text)?;
    if *skip >= edge.width as u64 {
        *skip -= edge.width as u64;
        return Ok(());
    }
    namespace_rank_page(&edge.left, skip, remaining, examine, emit)?;
    if *remaining == 0 {
        return Ok(());
    }
    if *skip == 0 {
        if !emit(&edge.name.text)? {
            *remaining = 0;
            return Ok(());
        }
        *remaining -= 1;
    } else {
        *skip -= 1;
    }
    namespace_rank_page(&edge.right, skip, remaining, examine, emit)
}
fn projected_tail(
    paths: &Namespace,
    path: &StreamPath,
    relative: Option<&StreamPath>,
    _work: CaptureWork<'_>,
) -> Result<(Option<u64>, Option<Record>), StreamError> {
    let Some(relative) = relative else {
        return Ok((None, None));
    };
    // A combined path over the bound cannot exist, so that child has no tail;
    // one long child never fails the page.
    if path.as_str().len() + 1 + relative.as_str().len() > crate::MAX_PATH_BYTES {
        return Ok((None, None));
    }
    let target = capture_child_path(Some(path), relative.as_str(), _work)?;
    let Some(stream) = paths.get(&target) else {
        return Ok((None, None));
    };
    #[cfg(not(target_arch = "wasm32"))]
    let record = if let Some(work) = _work {
        native_last_record(stream, work)?
    } else {
        last_record(stream)
    };
    #[cfg(target_arch = "wasm32")]
    let record = last_record(stream);
    if stream.tail != 0 && record.is_none() {
        return Err(StreamError::PrefixNotRetained);
    }
    Ok((Some(stream.tail), record))
}

fn capture_child_path(
    parent: Option<&StreamPath>,
    name: &str,
    _work: CaptureWork<'_>,
) -> Result<StreamPath, StreamError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = _work {
        return work.record(StreamPath::join_native(parent, name, &work.meter))?;
    }
    StreamPath::new(parent.map_or_else(|| name.to_owned(), |parent| format!("{parent}/{name}")))
}

fn last_record(stream: &PathState) -> Option<Record> {
    match last_record_with(
        stream,
        |_| Ok::<_, std::convert::Infallible>(()),
        |batch, index| Ok(batch.record(index)),
    ) {
        Ok(record) => record,
        Err(error) => match error {},
    }
}

fn last_record_with<E>(
    stream: &PathState,
    examine: impl FnMut(u64) -> Result<(), E>,
    retain: impl FnOnce(&RecordBatch, usize) -> Result<Option<Record>, E>,
) -> Result<Option<Record>, E> {
    let Some(from) = stream.tail.checked_sub(1) else {
        return Ok(None);
    };
    if let Some(window) = stream.prefix_window(from, stream.tail) {
        return retain(&window.records, window.start);
    }
    let mut history = PinnedHistory::new(stream, stream.tail);
    let window = history.page(from, 1, examine)?;
    match window {
        Some(window) => retain(&window.records, window.start),
        None => Ok(None),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_last_record(stream: &PathState, work: &Work) -> Result<Option<Record>, StreamError> {
    work.record(last_record_with(
        stream,
        |items| work.meter.examine(items, 0),
        |batch, index| {
            let record = batch
                .values
                .get(index)
                .ok_or(std::io::ErrorKind::InvalidData)?;
            let mut bytes = acyclic_native_runtime::NativeByteReservation::new(
                Arc::clone(&work.meter),
                NativeOperationKind::Read,
                record.payload_bytes(),
            )?;
            let copied = std::mem::size_of::<Record>() as u64;
            work.meter.admit_examination(1, copied)?;
            let output = batch.record(index).ok_or(std::io::ErrorKind::InvalidData)?;
            let read = bytes.record(record.payload_bytes());
            let examined = work.meter.record_examination(1, copied);
            read?;
            examined?;
            Ok(Some(output))
        },
    ))
}

fn children_at(
    paths: &Namespace,
    hierarchy_version: CommitId,
    request: &ChildrenPageRequest,
    limit: u32,
    remaining: usize,
    work: CaptureWork<'_>,
    _allocation: &mut Allocation,
) -> Result<ChildrenPage, StreamError> {
    request.validate()?;
    if request
        .hierarchy_version
        .is_some_and(|version| version != hierarchy_version)
    {
        return Err(StreamError::HierarchyChanged);
    }
    let children = request
        .parent
        .as_ref()
        .map_or(Some(&paths.roots), |parent| {
            paths.node(parent).map(|node| &node.children)
        });
    let empty = None;
    let index = children.unwrap_or(&empty);
    let lower = request
        .after
        .as_ref()
        .map(|path| path.as_str().rsplit('/').next().unwrap_or_default());
    let mut examine = |_name: &str| {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            return work.record(work.meter.examine(1, 0));
        }
        Ok(())
    };
    let page = namespace_page(index, lower, request.offset, limit as usize, &mut examine)?;
    #[cfg(not(target_arch = "wasm32"))]
    let mut rows = if let Some(work) = work {
        if page.length == 0 {
            Vec::new()
        } else {
            let original = _allocation.get_or_insert_with(|| ReadAllocation::empty(&work.meter));
            work.record(crate::native::vector(page.length, &work.meter, original))?
        }
    } else {
        child_buffer(page.length)?
    };
    #[cfg(target_arch = "wasm32")]
    let mut rows = child_buffer(page.length)?;
    let mut page_bytes = 0usize;
    let mut has_more = page.has_more;
    page.visit(&mut examine, &mut |name| {
        let path = capture_child_path(request.parent.as_ref(), name, work)?;
        let (tail, tail_record) = projected_tail(paths, &path, request.tail_stream.as_ref(), work)?;
        let origin = capture_origin(paths, &path, work)?;
        let child = Child {
            path,
            tail,
            tail_record,
            origin,
        };
        #[cfg(all(loom, not(target_arch = "wasm32")))]
        let size =
            if crate::verification::fault_is(crate::verification::Fault::IgnorePageRecordBytes) {
                child.path.as_str().len()
            } else {
                crate::wire_codec::requests::child_len(&child)
            };
        #[cfg(not(all(loom, not(target_arch = "wasm32"))))]
        let size = crate::wire_codec::requests::child_len(&child);
        let candidate_bytes =
            page_bytes.saturating_add(crate::wire_codec::requests::block_len(size));
        let continuation = (page.has_more || rows.len() + 1 < page.length).then_some(&child.path);
        if crate::wire_codec::requests::block_len(crate::wire_codec::requests::children_page_len(
            candidate_bytes,
            continuation,
            (rows.len() + 1) as u32,
        )) > remaining
        {
            if rows.is_empty() {
                return Err(StreamError::LimitExceeded);
            }
            has_more = true;
            return Ok(false);
        }
        page_bytes = candidate_bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.record(
                work.meter
                    .admit_examination(1, std::mem::size_of::<Child>() as u64),
            )?;
        }
        rows.push(child);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.record(
                work.meter
                    .record_examination(1, std::mem::size_of::<Child>() as u64),
            )?;
        }
        Ok(true)
    })?;
    let next_after = if has_more {
        rows.last().map(|child| child.path.clone())
    } else {
        None
    };
    if crate::wire_codec::requests::block_len(crate::wire_codec::requests::children_page_len(
        page_bytes,
        next_after.as_ref(),
        rows.len() as u32,
    )) > remaining
    {
        return Err(StreamError::LimitExceeded);
    }
    Ok(ChildrenPage {
        scanned: rows.len() as u32,
        hierarchy_version,
        children: rows,
        next_after,
    })
}

fn child_buffer(length: usize) -> Result<Vec<Child>, StreamError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(length)
        .map_err(|_| StreamError::Capacity)?;
    Ok(rows)
}
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StreamProvider for MemoryStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.operation_claims).with_process_exclusion()
    }

    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.heads",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            self.heads_inner(request)
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
            self.inspect_idempotency_inner(idempotency_key)
        )
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        let accepted = matches!(&request, crate::WriteRequest::CommitAccepted(_));
        match request {
            crate::WriteRequest::Append(request) => {
                Box::pin(acyclic_observability::operation_future!(
                    "acyclic.stream.append",
                    StreamError::code,
                    acyclic_native_runtime::obs::OperationSpan::new,
                    async move {
                        self.append_inner(request)
                            .await
                            .map(|(outcome, _)| crate::WriteOutcome::Append(outcome))
                    }
                ))
            }
            crate::WriteRequest::Fork(request) => {
                Box::pin(acyclic_observability::operation_future!(
                    "acyclic.stream.fork",
                    StreamError::code,
                    acyclic_native_runtime::obs::OperationSpan::new,
                    async move {
                        let _publication = self.publication.lock().await;
                        let mut state = self.state.write().await;
                        self.plan_fork_locked(&mut state, &request)?
                            .install(self, &mut state, false)
                            .map(crate::WriteOutcome::Fork)
                    }
                ))
            }
            crate::WriteRequest::Commit(request) | crate::WriteRequest::CommitAccepted(request) => {
                Box::pin(acyclic_observability::operation_future!(
                    "acyclic.stream.commit",
                    StreamError::code,
                    acyclic_native_runtime::obs::OperationSpan::new,
                    async move {
                        let mut request = request;
                        let digest = self.commit_request_digest(&mut request)?;
                        let _publication = self.publication.lock().await;
                        let mut state = self.state.write().await;
                        self.plan_commit_locked(&mut state, &request, digest, None, accepted)?
                            .install(self, &mut state, false)
                            .map(|(outcome, _)| outcome)
                    }
                ))
            }
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.read",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            async { self.read_inner(request.validate()?).await }
        )
    }

    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.follow",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            self.follow_inner(request)
        )
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        acyclic_observability::async_operation!(
            "acyclic.stream.read_commit",
            StreamError::code,
            acyclic_native_runtime::obs::OperationSpan::new,
            self.read_commit_inner(commit_id)
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::NativeStreamProvider for MemoryStream {
    fn native_follow(
        &self,
        request: crate::FollowRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::NativeRecordStream> {
        if let Err(error) = crate::native::validate_follow(&request) {
            return crate::NativeProviderFuture::ready_observed(
                crate::native::span!("acyclic.stream.follow"),
                Ok(Err(error)),
                StreamError::code,
            );
        }
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::observed(
            &original,
            crate::native::span!("acyclic.stream.follow"),
            async move {
                let work = Work::new(meter)?;
                let provider = self.with_context(Arc::clone(&work));
                let result = provider
                    .follow_inner(request)
                    .await
                    .and_then(|stream| crate::native::follow_records(stream, &work));
                work.finish(result.map(|stream| (stream, None)))
            },
            |result| crate::native::outcome(result, StreamError::code),
        )
    }
    fn native_heads(
        &self,
        request: crate::HeadsRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::Heads> {
        if let Err(error) = Self::validate_native_heads(&request) {
            return crate::NativeProviderFuture::ready_observed(
                crate::native::span!("acyclic.stream.heads"),
                Ok(Err(error)),
                StreamError::code,
            );
        }
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::observed(
            &original,
            crate::native::span!("acyclic.stream.heads"),
            async move {
                let work = Work::inline(meter);
                let result = async {
                    self.revisions.wait(&request.at_least).await?;
                    self.capture_native_heads(&request, &work).await
                }
                .await;
                work.finish(result)
            },
            |result| crate::native::outcome(result, StreamError::code),
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::NativeRecordStream> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::observed(
            &original,
            crate::native::span!("acyclic.stream.read"),
            async move {
                let request = match request.validate() {
                    Ok(request) => request,
                    Err(error) => return Ok(Err(error)),
                };
                self.native_read_inner(request, meter).await
            },
            |result| crate::native::outcome(result, StreamError::code),
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, u64> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::observed(
            &original,
            crate::native::span!("acyclic.stream.tail"),
            self.native_tail_inner(path, meter),
            |result| crate::native::outcome(result, StreamError::code),
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let original = Arc::clone(&meter);
        crate::NativeProviderFuture::observed(
            &original,
            crate::native::span!("acyclic.stream.inspect_idempotency"),
            self.native_inspect_idempotency_inner(key, meter),
            |result| crate::native::outcome(result, StreamError::code),
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_guarded(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: crate::NativeCommitGuard,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome, crate::NativeCommitError> {
        let (reads, check) = before.into_parts();
        self.guarded_commit(
            request,
            deadline,
            meter,
            check,
            Some(reads),
            false,
            crate::WriteOutcome::into_commit,
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        capacity: crate::PreparedRecordCapacity,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        Work::start(
            meter,
            crate::native::span!("acyclic.stream.prepare_commit"),
            |work, completion| {
                completion.observe_output(
                    async move {
                        let provider = self.with_context(Arc::clone(&work));
                        let result = async {
                            let (plan, recovery) = provider.prepare(request, capacity).await?;
                            work.reserve_write(capacity.payload_bytes)?;
                            let slot = work
                                .record(crate::native::NativeCommitRecordSlot::new(&work.meter))?;
                            let ingress = Arc::clone(&slot);
                            let writer_work = Arc::clone(&work);
                            let future = work.record(crate::NativeProviderFuture::try_observed(
                                &work.meter,
                                crate::native::span!("acyclic.stream.commit"),
                                async move {
                                    let result = match ingress.receive().await {
                                        Ok(body) => plan.settle(body).await,
                                        Err(error) => Err(error),
                                    };
                                    writer_work.finish(result)
                                },
                                |result| crate::native::outcome(result, StreamError::code),
                            ))?;
                            Ok((
                                crate::PreparedNativeCommit::new(future, recovery, slot, capacity),
                                None,
                            ))
                        }
                        .await;
                        work.finish(result)
                    },
                    |result| crate::native::outcome(result, StreamError::code),
                )
            },
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_write(
        &self,
        request: crate::WriteRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::WriteOutcome> {
        let accepted = matches!(&request, crate::WriteRequest::CommitAccepted(_));
        match request {
            crate::WriteRequest::Append(request) => Work::start(
                meter,
                crate::native::span!("acyclic.stream.append"),
                |work, completion| {
                    completion.observe_output(
                        async move {
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
                                |(outcome, allocation)| {
                                    (crate::WriteOutcome::Append(outcome), allocation)
                                },
                            ))
                        },
                        |result| crate::native::outcome(result, StreamError::code),
                    )
                },
            ),
            crate::WriteRequest::Commit(request) | crate::WriteRequest::CommitAccepted(request) => {
                self.guarded_commit(
                    request,
                    None,
                    meter,
                    || futures::future::ready(Ok(())),
                    None,
                    accepted,
                    Ok,
                )
            }
            crate::WriteRequest::Fork(_) => crate::NativeProviderFuture::ready_observed(
                crate::native::span!("acyclic.stream.fork"),
                Ok(Err(StreamError::Unsupported)),
                StreamError::code,
            ),
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
        reads: Option<Vec<StreamPath>>,
        accepted: bool,
        convert: C,
    ) -> crate::NativeProviderFuture<'_, T, E>
    where
        T: Send + 'static,
        C: FnOnce(crate::WriteOutcome) -> Result<T, StreamError> + Send + 'static,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        Work::start(
            meter,
            crate::native::span!("acyclic.stream.commit"),
            |work, completion| {
                completion.observe_output(
                    async move {
                        let provider = self.with_context(Arc::clone(&work));
                        let result = async {
                            let _input = provider.request_claim(&request).map_err(E::from)?;
                            let _reads = provider
                                .reads_claim(reads.as_ref().map_or(0, Vec::capacity))
                                .map_err(E::from)?;
                            let digest = provider.digest(&mut request).map_err(E::from)?;
                            let cut = {
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
                                    let (receipt, mut claim) = provider
                                        .replayed(
                                            &request,
                                            value,
                                            provider.revisions.at(replay.index),
                                        )
                                        .map_err(E::from)?;
                                    let envelope = provider
                                        .selected_envelope(value, accepted, &mut claim)
                                        .map_err(E::from)?;
                                    return commit_projection(
                                        CommitOutcome::Committed(receipt),
                                        envelope,
                                    )
                                    .map(|outcome| (outcome, claim))
                                    .map_err(E::from);
                                }
                                // An unguarded Commit observes nothing before
                                // its locked plan, which rechecks its key.
                                match &reads {
                                    Some(reads) => Some(
                                        provider
                                            .guard_cut_locked(&state, &request, reads)
                                            .map_err(E::from)?,
                                    ),
                                    None => None,
                                }
                            };
                            before().await?;
                            let _publication = provider.publication.lock().await;
                            let mut state = provider.state.write().await;
                            if let (Some(cut), Some(reads)) = (cut, &reads)
                                && provider
                                    .guard_cut_locked(&state, &request, reads)
                                    .map_err(E::from)?
                                    != cut
                            {
                                return Err(E::from(StreamError::PrefixNotRetained));
                            }
                            let bytes = request
                                .mutations()
                                .map(|mutation| match mutation {
                                    CommitMutation::Append { records, .. }
                                    | CommitMutation::Fork { records, .. } => records
                                        .iter()
                                        .map(AppendRecord::payload_bytes)
                                        .sum::<usize>(),
                                })
                                .sum();
                            work.reserve_write(bytes).map_err(E::from)?;
                            provider
                                .plan_commit_locked(
                                    &mut state, &request, digest, deadline, accepted,
                                )
                                .and_then(|plan| plan.install(&provider, &mut state, false))
                                .map_err(E::from)
                        }
                        .await;
                        work.finish(result.and_then(|(outcome, allocation)| {
                            Ok((convert(outcome).map_err(E::from)?, allocation))
                        }))
                    },
                    |result| crate::native::outcome(result, |_| "guarded"),
                )
            },
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryStream {
    pub(crate) fn validate_native_heads(request: &crate::HeadsRequest) -> Result<(), StreamError> {
        request.validate()?;
        if !matches!(
            request.projection,
            crate::HeadProjection::Tail | crate::HeadProjection::History
        ) || request.durable_through.is_some()
            || !request.retained.is_empty()
        {
            return Err(StreamError::Unsupported);
        }
        Ok(())
    }

    /// Captures the original request after its floor is satisfied. The output
    /// owns its spill before allocation and through every failure and return.
    pub(crate) async fn capture_native_heads(
        &self,
        request: &crate::HeadsRequest,
        work: &Work,
    ) -> Result<(crate::Heads, Allocation), StreamError> {
        // The value is declared last so refusal drops all page storage before
        // the original child-vector credit, including during unwind.
        let mut children_allocation = None;
        let mut output = crate::NativeStreamOutput::new(
            self.empty_heads(request),
            ReadAllocation::empty(&work.meter),
        );
        let length = request.paths.len();
        if length > output.value.heads.inline_size() {
            let bytes = length
                .checked_mul(std::mem::size_of::<Option<crate::PathHead>>())
                .ok_or(StreamError::LimitExceeded)?;
            output.allocation = work.record(ReadAllocation::new(&work.meter, bytes))?;
            work.record(
                output
                    .value
                    .heads
                    .try_reserve_exact(length)
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory)),
            )?;
            work.record(output.allocation.reconcile_capacity(
                output.value.heads.capacity() * std::mem::size_of::<Option<crate::PathHead>>(),
            ))?;
        }
        work.record(work.meter.check())?;
        self.capture_heads_into(
            request,
            HeadDurability::Volatile,
            &mut output.value,
            Some(work),
            &mut children_allocation,
        )
        .await?;
        if let Some(mut children) = children_allocation.take() {
            if let Err(error) = output.allocation.absorb(&mut children) {
                drop(output);
                drop(children);
                return work.record(Err(error));
            }
        }
        work.record(work.meter.check())?;
        let (view, allocation) = output.into_parts();
        Ok((view, Some(allocation)))
    }

    pub(crate) async fn native_read_inner(
        &self,
        request: crate::ValidatedReadRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeStreamResult<crate::NativeRecordStream> {
        if let Err(error) = self.revisions.wait(&request.request().at_least).await {
            return Ok(Err(error));
        }
        let request = request.into_request();
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
        let snapshot = self.revisions.at(state.decision);
        let selection = cursor.selection();
        drop(state);
        let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&meter);
        let cursor_meter = Arc::clone(&meter);
        let cursor = stream::try_unfold(cursor, move |mut cursor| {
            let meter = Arc::clone(&cursor_meter);
            async move {
                let Some(record) = cursor.peek(
                    |items| meter.examine(items, 0),
                    || std::io::ErrorKind::InvalidData.into(),
                )?
                else {
                    return Ok(None);
                };
                let length = record.value.len();
                let record = cursor.take_native_record(&meter, length)?;
                Ok(Some((
                    Ok(crate::NativeStreamOutput::new(
                        record,
                        ReadAllocation::empty(&meter),
                    )),
                    cursor,
                )))
            }
        });
        let mut frame =
            acyclic_native_runtime::ReadAllocation::new(&meter, std::mem::size_of_val(&cursor))?;
        allocation.absorb(&mut frame)?;
        let cursor = crate::Opened::new(snapshot, selection, Box::pin(cursor));
        Ok(Ok(crate::NativeStreamOutput::new(cursor, allocation)))
    }
    pub(crate) async fn native_tail_inner(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeStreamResult<u64> {
        let state = self.state.read().await;
        let value = inspect(&meter, || state.tail(&path))?;
        Ok(value
            .ok_or(StreamError::NotFound)
            .map(|value| crate::NativeStreamOutput::new(value, ReadAllocation::empty(&meter))))
    }
    pub(crate) async fn native_inspect_idempotency_inner(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> crate::NativeStreamResult<Option<IdempotencyObservation>> {
        let state = self.state.read().await;
        let replay = inspect(&meter, || {
            state.replay(key.as_bytes(), |items| meter.examine(items, 0))
        })??;
        let output = match replay {
            Some(replay) => crate::native::clone_outcome(&replay.result, &meter)?.map(|outcome| {
                Some(IdempotencyObservation {
                    idempotency_key: key,
                    request_digest: replay.digest,
                    outcome,
                    revision: Some(self.revisions.at(replay.index)),
                })
            }),
            None => crate::NativeStreamOutput::new(None, ReadAllocation::empty(&meter)),
        };
        meter.check()?;
        Ok(Ok(output))
    }
}
