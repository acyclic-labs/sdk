struct LatestRead {
    from: u64,
    key: Option<ReadKeySeek>,
    before: u64,
}

impl LatestRead {
    fn new(
        path: StreamPath,
        key: Option<Bytes>,
        before: u64,
        at_least: RevisionCut,
    ) -> Option<(Self, ReadRequest)> {
        let last = before.checked_sub(1)?;
        let key = key.map(|key| ReadKeySeek::latest(key, before));
        let from = if key.is_some() { 0 } else { last };
        Some((
            Self {
                from,
                key: key.clone(),
                before,
            },
            ReadRequest {
                from,
                key,
                path,
                limit: 1,
                at_least,
            },
        ))
    }

    fn validate(&mut self, record: &Record) -> Result<(), StreamError> {
        if record.sequence >= self.before {
            return Err(StreamError::InvalidResponse);
        }
        match &mut self.key {
            Some(selector) => {
                selector.advance(&mut self.from, record.sequence, record.headers.keys())
            }
            None if record.sequence == self.from => Ok(()),
            None => Err(StreamError::InvalidResponse),
        }
    }
}

/// Projections of canonical provider operations, shared by every backend.
///
/// These methods introduce no provider dispatch or independent observation.
pub trait StreamProviderExt: StreamProvider {
    /// Selects one indexed record before an original cut on its native owner.
    /// The finite cursor must close successfully before its record is returned;
    /// the result retains that record's original metadata allocation. Zero is
    /// known absence without provider IO. Unsupported native reads never fall
    /// back to the portable provider entrance.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_latest_before(
        &self,
        path: StreamPath,
        key: Option<Bytes>,
        before: u64,
        at_least: RevisionCut,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> impl Future<Output = NativeStreamResult<Option<Record>>> + Send
    where
        Self: NativeStreamProvider,
    {
        async move {
            use futures::TryStreamExt as _;
            meter.check()?;
            if before == 0 {
                return Ok(Ok(NativeStreamOutput::new(
                    None,
                    acyclic_native_runtime::ReadAllocation::empty(&meter),
                )));
            }
            let key = key
                .map(|key| key.into_cloneable_native(&meter))
                .transpose()?;
            let (mut selection, request) = LatestRead::new(path, key, before, at_least)
                .expect("nonzero original cut has one predecessor");
            let mut records = match self.native_read(request, meter.clone()).await? {
                Ok(records) => records,
                Err(error) => return Ok(Err(error)),
            };
            let record = match records.value_mut().try_next().await? {
                Some(Ok(record)) => Some(record),
                Some(Err(error)) => return Ok(Err(error)),
                None => None,
            };
            if let Some(record) = &record
                && let Err(error) = selection.validate(record.value())
            {
                return Ok(Err(error));
            }
            if record.is_some() {
                match records.value_mut().try_next().await? {
                    Some(Err(error)) => return Ok(Err(error)),
                    Some(Ok(_)) => return Ok(Err(StreamError::InvalidResponse)),
                    None => {}
                }
            }
            meter.check()?;
            Ok(Ok(match record {
                Some(record) => record.map(Some),
                None => NativeStreamOutput::new(
                    None,
                    acyclic_native_runtime::ReadAllocation::empty(&meter),
                ),
            }))
        }
    }

    /// Projects funded append publication without another future allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> impl Future<Output = NativeStreamResult<AppendOutcome>> + Send
    where
        Self: NativeStreamProvider;
    /// Projects funded commit publication without another future allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> impl Future<Output = NativeStreamResult<CommitOutcome>> + Send
    where
        Self: NativeStreamProvider;

    /// Canonical append publication, preserving its genuine retry family.
    fn append(
        &self,
        request: AppendRequest,
    ) -> impl std::future::Future<Output = Result<AppendOutcome, StreamError>> + ProviderTask;
    /// Canonical fork publication, preserving its genuine retry family.
    fn fork(
        &self,
        request: ForkRequest,
    ) -> impl std::future::Future<Output = Result<ForkReceipt, StreamError>> + ProviderTask;
    /// Canonical commit publication, preserving its genuine retry family.
    /// Results, including skipped soft appends, follow canonical path order
    /// from [`crate::request::validate_commit`], not caller mutation order.
    fn commit(
        &self,
        request: CommitRequest,
    ) -> impl std::future::Future<Output = Result<CommitOutcome, StreamError>> + ProviderTask;

    /// Observes one named head at a coherent authority snapshot.
    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask;

    /// Current accepted next sequence, without loading history or awaiting a flush.
    fn tail(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<u64, StreamError>> + ProviderTask;

    /// Accepted visibility and its confirmed durable prefix at one snapshot.
    fn watermarks(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<StreamWatermarks, StreamError>> + ProviderTask;

    /// Publishes the same canonical commit and selects its original accepted
    /// envelope. An unsupported adapter refuses before effects; this operation
    /// never emulates acceptance with a post-write read or caller request bytes.
    fn commit_accepted(
        &self,
        request: CommitRequest,
    ) -> impl std::future::Future<Output = Result<AcceptedCommitOutcome, StreamError>> + ProviderTask
    {
        async move {
            match self.write(WriteRequest::CommitAccepted(request)).await? {
                WriteOutcome::CommitAccepted(outcome) => Ok(outcome),
                _ => Err(StreamError::InvalidResponse),
            }
        }
    }

    /// Confirms an irreversible prefix at one captured view. A retained memory
    /// prefix is valid without asserting crash persistence.
    fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> impl std::future::Future<Output = Result<Option<DurableReceipt>, StreamError>> + ProviderTask;

    /// One bounded direct-child page at a coherent authority snapshot: the
    /// children read selector. Pages resume at next_after.
    /// after without hierarchy_version is a valid exclusive seek within the
    /// current snapshot; the provider must accept it.
    fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> impl std::future::Future<Output = Result<ChildrenPage, StreamError>> + ProviderTask;

    /// The latest record carrying `key` at or after `at_least`, by one
    /// latest-first indexed lookup, never a scan of the stream.
    fn latest(
        &self,
        path: StreamPath,
        key: Bytes,
        at_least: RevisionCut,
    ) -> impl std::future::Future<Output = Result<Option<Record>, StreamError>> + ProviderTask;

    /// One indexed point read strictly before an original exclusive cut.
    /// No head read is needed when the caller already owns that cut. `None`
    /// selects the last unkeyed record; zero is known absence without IO.
    fn latest_before(
        &self,
        path: StreamPath,
        key: Option<Bytes>,
        before: u64,
        at_least: RevisionCut,
    ) -> impl Future<Output = Result<Option<Record>, StreamError>> + ProviderTask {
        async move {
            use futures::StreamExt as _;
            if before == 0 {
                return Ok(None);
            }
            #[cfg(feature = "serde")]
            let diagnostic = crate::trace::is_source(&path);
            let work = async move {
                let mut selector = key.map(|key| ReadKeySeek::latest(key, before));
                let mut from = if selector.is_some() { 0 } else { before - 1 };
                let mut records = self
                    .read(ReadRequest {
                        key: selector.clone(),
                        path,
                        from,
                        limit: 1,
                        at_least,
                    })
                    .await?;
                let record = records.next().await.transpose()?;
                if let Some(record) = &record {
                    if record.sequence >= before {
                        return Err(StreamError::InvalidResponse);
                    }
                    if let Some(selector) = &mut selector {
                        selector.advance(&mut from, record.sequence, record.headers.keys())?;
                    }
                }
                Ok(record)
            };
            #[cfg(feature = "serde")]
            if diagnostic {
                return work.await;
            }
            #[cfg(feature = "serde")]
            return crate::trace::observe(
                crate::trace::Request {
                    name: "stream.latest_before",
                    mechanism: crate::trace::Mechanism::ProviderCall,
                    parent: None,
                    guarantee: Some("indexed_record_strictly_before_original_cut"),
                },
                work,
                |record| {
                    Some(
                        record
                            .as_ref()
                            .map_or(0, |record| record.value.len() as u64),
                    )
                },
            )
            .await;
            #[cfg(not(feature = "serde"))]
            work.await
        }
    }

    /// Select the latest record, optionally keyed, at a captured head with the condition that
    /// fences that observation in the caller's original publication. The head
    /// read buys this optimistic fence; the bounded key read cannot include a
    /// later append. Absence is fenced too. Neither operation scans history.
    fn latest_fenced(
        &self,
        path: StreamPath,
        key: Option<Bytes>,
        at_least: RevisionCut,
    ) -> impl Future<Output = Result<(Option<Record>, CommitCondition), StreamError>> + ProviderTask
    {
        async move {
            let request = HeadsRequest {
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::Tail,
                paths: std::iter::once(path.clone()).collect(),
                children: None,
                limit: 1,
                at_least,
            };
            let work = async {
                let heads = self.heads(request.clone()).await?;
                heads.validate_for(&request)?;
                Ok::<_, StreamError>(heads)
            };
            #[cfg(feature = "serde")]
            let heads = if crate::trace::is_source(&path) {
                work.await?
            } else {
                crate::trace::observe(
                    crate::trace::Request {
                        name: "stream.latest_fence",
                        mechanism: crate::trace::Mechanism::ProviderCall,
                        parent: None,
                        guarantee: Some("original_head_cut_for_optimistic_publication_fence"),
                    },
                    work,
                    |_| None,
                )
                .await?
            };
            #[cfg(not(feature = "serde"))]
            let heads = work.await?;
            let (before, condition) = match heads.heads.first().and_then(Option::as_ref) {
                Some(head) => (
                    head.tail,
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected: head.tail,
                    },
                ),
                None => (0, CommitCondition::Absent { path: path.clone() }),
            };
            let record = self
                .latest_before(path, key, before, RevisionCut::of(heads.snapshot))
                .await?;
            Ok((record, condition))
        }
    }

    /// Waits for the authority snapshot to cover the caller's minimum cut.
    fn wait_applied(
        &self,
        at_least: &RevisionCut,
    ) -> impl std::future::Future<Output = Result<Revision, StreamError>> + ProviderTask;
}

impl<P: StreamProvider + ?Sized> StreamProviderExt for P {
    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> impl Future<Output = NativeStreamResult<AppendOutcome>> + Send
    where
        Self: NativeStreamProvider,
    {
        let future = self.native_write(WriteRequest::Append(request), meter);
        async move {
            let result = future.await?;
            Ok(result.and_then(|output| {
                let (outcome, allocation) = output.into_parts();
                match outcome {
                    WriteOutcome::Append(outcome) => {
                        Ok(NativeStreamOutput::new(outcome, allocation))
                    }
                    _ => Err(StreamError::InvalidResponse),
                }
            }))
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        mut request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> impl Future<Output = NativeStreamResult<CommitOutcome>> + Send
    where
        Self: NativeStreamProvider,
    {
        request.deadline_unix_millis = request
            .deadline_unix_millis
            .into_iter()
            .chain(deadline)
            .min();
        let future = self.native_write(WriteRequest::Commit(request), meter);
        async move {
            let result = future.await?;
            Ok(result.and_then(|output| {
                let (outcome, allocation) = output.into_parts();
                match outcome {
                    WriteOutcome::Commit(outcome) => {
                        Ok(NativeStreamOutput::new(outcome, allocation))
                    }
                    _ => Err(StreamError::InvalidResponse),
                }
            }))
        }
    }

    fn append(
        &self,
        request: AppendRequest,
    ) -> impl std::future::Future<Output = Result<AppendOutcome, StreamError>> + ProviderTask {
        async move {
            match self.write(WriteRequest::Append(request)).await? {
                WriteOutcome::Append(outcome) => Ok(outcome),
                _ => Err(StreamError::InvalidResponse),
            }
        }
    }
    fn fork(
        &self,
        request: ForkRequest,
    ) -> impl std::future::Future<Output = Result<ForkReceipt, StreamError>> + ProviderTask {
        async move {
            match self.write(WriteRequest::Fork(request)).await? {
                WriteOutcome::Fork(outcome) => Ok(outcome),
                _ => Err(StreamError::InvalidResponse),
            }
        }
    }
    fn commit(
        &self,
        request: CommitRequest,
    ) -> impl std::future::Future<Output = Result<CommitOutcome, StreamError>> + ProviderTask {
        async move {
            match self.write(WriteRequest::Commit(request)).await? {
                WriteOutcome::Commit(outcome) => Ok(outcome),
                _ => Err(StreamError::InvalidResponse),
            }
        }
    }

    fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> impl std::future::Future<Output = Result<Option<DurableReceipt>, StreamError>> + ProviderTask
    {
        async move {
            let request = HeadsRequest {
                at: None,
                retained: Default::default(),
                durable_through: Some(through),
                projection: HeadProjection::Watermarks,
                paths: [path.clone()].into(),
                children: None,
                limit: 1,
                at_least: Default::default(),
            };
            let mut view = self.heads(request.clone()).await?;
            view.validate_for(&request)?;
            let head = view.heads.pop().flatten().ok_or(StreamError::NotFound)?;
            Ok(head
                .retained_through
                .map(|through| DurableReceipt { path, through }))
        }
    }

    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask {
        named_head(self, path, HeadProjection::History)
    }

    fn tail(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<u64, StreamError>> + ProviderTask {
        async move { Ok(named_head(self, path, HeadProjection::Tail).await?.tail) }
    }

    fn watermarks(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<StreamWatermarks, StreamError>> + ProviderTask
    {
        async move {
            let head = named_head(self, path, HeadProjection::Watermarks).await?;
            Ok(StreamWatermarks {
                accepted: head.tail,
                durable: head.durable,
            })
        }
    }

    fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> impl std::future::Future<Output = Result<ChildrenPage, StreamError>> + ProviderTask {
        async move {
            let selection = HeadsRequest {
                at: None,
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::Watermarks,
                paths: Default::default(),
                limit: request.limit,
                children: Some(request),
                at_least: Default::default(),
            };
            let view = self.heads(selection.clone()).await?;
            view.validate_for(&selection)?;
            view.children.ok_or(StreamError::InvalidResponse)
        }
    }

    fn latest(
        &self,
        path: StreamPath,
        key: Bytes,
        at_least: RevisionCut,
    ) -> impl std::future::Future<Output = Result<Option<Record>, StreamError>> + ProviderTask {
        self.latest_before(path, Some(key), u64::MAX, at_least)
    }

    fn wait_applied(
        &self,
        at_least: &RevisionCut,
    ) -> impl std::future::Future<Output = Result<Revision, StreamError>> + ProviderTask {
        async move {
            let view = self
                .heads(HeadsRequest {
                    at: None,
                    retained: Default::default(),
                    durable_through: None,
                    projection: Default::default(),
                    paths: Default::default(),
                    children: None,
                    limit: 1,
                    at_least: at_least.clone(),
                })
                .await?;
            if view.projection != HeadProjection::History
                || !view.heads.is_empty()
                || view.children.is_some()
                || !at_least.proven_by(Some(&view.snapshot))
            {
                return Err(StreamError::InvalidResponse);
            }
            Ok(view.snapshot)
        }
    }
}

async fn named_head<P: StreamProvider + ?Sized>(
    provider: &P,
    path: StreamPath,
    projection: HeadProjection,
) -> Result<PathHead, StreamError> {
    let request = HeadsRequest {
        at: None,
        retained: Default::default(),
        durable_through: None,
        projection,
        paths: [path].into(),
        children: None,
        limit: 1,
        at_least: Default::default(),
    };
    let mut view = provider.heads(request.clone()).await?;
    view.validate_for(&request)?;
    view.heads.pop().flatten().ok_or(StreamError::NotFound)
}
