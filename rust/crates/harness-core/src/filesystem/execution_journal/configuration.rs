use super::*;

#[cfg(all(test, not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod tests;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn invalidate_execution_caches(&mut self) {
        self.cache_identity = Default::default();
        *self.verified.get_mut() = SummaryCache::default();
        *self.conversation_projection.get_mut() = None;
    }

    /// Used only after the runtime authenticates the task's retained admission.
    pub(in crate::filesystem) fn for_approval_projection(mut self, task: crate::TaskId) -> Self {
        self.approval_task = Some(task);
        self.invalidate_execution_caches();
        self
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn original_native_execution_work_matches(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> bool {
        self._native_metadata
            .as_ref()
            .is_some_and(|custody| work.shares_ledger(custody.work()))
    }

    /// Checks the task lease, execution and coordinator bindings.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn original_task_execution_binding_matches(
        &self,
        task: crate::TaskId,
        fence: &crate::scheduler::LeaseFence,
        turn: OperationId,
        operation: OperationId,
        host: &crate::durable_host::CoordinatorTaskHost<P>,
    ) -> bool {
        self.execution
            .as_ref()
            .is_some_and(|identity| identity.0 == operation && identity.1 == turn)
            && self.owner.as_ref().is_some_and(|(owner, bound)| {
                *bound == operation
                    && owner.matches_task_binding(task, fence)
                    && owner.original_host_binding_matches(host)
            })
    }

    pub(crate) fn with_optional_task_files(
        mut self,
        files: Option<Arc<crate::task_volume::TaskFiles<P>>>,
    ) -> Result<Self> {
        if let Some(files) = files {
            self.with_task_files(files)
        } else {
            self.task_files = None;
            self.invalidate_execution_caches();
            Ok(self)
        }
    }

    /// Attaches an owner-authorized private file projection on this stream provider.
    pub fn with_task_files(mut self, files: Arc<crate::task_volume::TaskFiles<P>>) -> Result<Self> {
        if files.volume().provider() != &self.host.provider
            || !files.inner.shares_provider(&self.stream)
            || files.maximum_bytes() < self.maximum_payload_bytes
        {
            #[cfg(not(target_arch = "wasm32"))]
            if self._native_metadata.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "task files differ from the journal's provider or byte bound",
                });
            }
            return Err(Error::Unauthorized(
                "task files differ from the journal's provider or byte bound".into(),
            ));
        }
        self.task_files = Some(files);
        self.invalidate_execution_caches();
        Ok(self)
    }

    /// Binds the execution to the task's exact lease and coordinator provider.
    pub fn for_task(
        owner: TaskJournalOwner<P>,
        operation_id: OperationId,
        turn_operation: OperationId,
        host: Arc<FilesystemHost<A, O>>,
        binding: super::super::FilesystemJournalBinding<'_>,
    ) -> Result<Self> {
        let super::super::FilesystemJournalBinding {
            volume,
            verifier,
            scope,
            maximum_bytes: maximum_payload_bytes,
            io,
        } = binding;
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        owner.validate_storage(verifier, maximum_payload_bytes, &mut work)?;
        let mut journal = Self::new(
            owner.stream(),
            host,
            &SchemaRegistry::new(),
            super::super::FilesystemJournalBinding {
                volume,
                verifier,
                scope,
                maximum_bytes: maximum_payload_bytes,
                io,
            },
        )?;
        journal.owner = Some((owner, operation_id));
        journal.execution = Some((operation_id, turn_operation));
        Ok(journal)
    }

    /// Binds passive replay to an original turn without granting a task lease.
    #[doc(hidden)]
    pub fn with_turn(mut self, operation: OperationId, turn: OperationId) -> Self {
        self.execution = Some((operation, turn));
        self.invalidate_execution_caches();
        self
    }

    pub(super) fn turn_operation(&self, operation: OperationId) -> OperationId {
        self.execution
            .as_ref()
            .map_or(operation, |identity| identity.1)
    }

    /// Admits the SDK Arc attempt while keeping metadata custody intrinsic.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        mut self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self
            ._native_metadata
            .as_ref()
            .is_some_and(|custody| custody.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native journal sharing requires its original funded metadata",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let mut body = work
            .claim(
                std::mem::size_of::<Self>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(Error::from)?;
        self._native_metadata
            .as_mut()
            .ok_or_else(|| io.invalid("original funded journal metadata is missing"))?
            .absorb(&mut body)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: Some(body),
            })?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }

    pub(super) fn require_operation(
        &self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if self
            .execution
            .as_ref()
            .is_some_and(|bound| bound.0 != operation)
        {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "journal belongs to another task execution",
            ));
        }
        Ok(())
    }

    /// Binds the pinned extension registry with optional original preparation IO.
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        schemas: &SchemaRegistry,
        binding: super::super::FilesystemJournalBinding<'_>,
    ) -> Result<Self> {
        let super::super::FilesystemJournalBinding {
            volume,
            verifier,
            scope,
            maximum_bytes: maximum_payload_bytes,
            io,
        } = binding;
        let invalid = |reason: &'static str| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some_and(|io| io.original_native_work().is_some()) {
                return Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason,
                };
            }
            Error::Invalid(reason.into())
        };
        crate::model::charge_items(io, 1)?;
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_payload_bytes == 0
        {
            return Err(invalid(
                "execution journal requires a private volume and positive limit",
            ));
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let maximum = io
                .map(|io| io.remaining().map(|budget| budget.bytes_encoded))
                .transpose()?
                .unwrap_or(u64::MAX);
            crate::executor::with_history_io(io, |work| {
                ContentGrant::authorize_volume(verifier, scope, volume, operation, maximum, work)
            })?;
        }
        if !matches!(
            verifier.audience().kind,
            crate::core::AggregateKind::Conversation | crate::core::AggregateKind::Task
        ) {
            return Err(invalid(
                "execution journal requires a task or conversation authority",
            ));
        }
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let volume = volume.copy_in_quota(&mut quota)?;
        let verifier = verifier.copy_in_quota(&mut quota)?;
        let schemas = schemas.copy(quota.work())?;
        let scope = scope.copy_in_quota(&mut quota)?;
        let interactions = match verifier.audience().kind {
            crate::core::AggregateKind::Conversation => Some(FilesystemInteractionHost::new(
                stream.clone(),
                Arc::clone(&host),
                verifier.audience(),
                &schemas,
                crate::filesystem::FilesystemJournalBinding {
                    volume: &volume,
                    verifier: &verifier,
                    scope: &scope,
                    maximum_bytes: maximum_payload_bytes,
                    io,
                },
            )?),
            crate::core::AggregateKind::Task => None,
            _ => unreachable!("execution authority validated before assembly"),
        };
        let _native_metadata = quota.into_custody()?;
        Ok(Self {
            cache_identity: Default::default(),
            stream,
            host,
            volume,
            verifier,
            scope,
            schemas,
            maximum_payload_bytes,
            input_verifier: None,
            bound_selection: None,
            stop: None,
            agent_approvals: None,
            interactions,
            owner: None,
            approval_task: None,
            execution: None,
            verified: tokio::sync::Mutex::new(SummaryCache::default()),
            conversation_projection: tokio::sync::Mutex::new(None),
            history_read_limits: None,
            task_files: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata,
        })
    }

    /// Binds tickets to the conversation owner with optional original preparation IO.
    pub fn with_interaction_owner(
        mut self,
        verifier: &AuthorityVerifier,
        schemas: &SchemaRegistry,
        scope: &Scope,
        private_volume: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        // Task approvals are followed as task files: an owner-bound task journal
        // without them could never prove a decision published.
        if self.owner.is_some() && self.task_files.is_none() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "task interaction owner requires private task files",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            let guard = self
                ._native_metadata
                .as_mut()
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native interaction binding requires original journal custody",
                })?;
            let mut identity = io.take_native_custody(0)?;
            guard
                .absorb(&mut identity)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: Some(identity),
                })?;
        }
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
        } else if self.approval_task.is_none()
            && self.verifier.audience().kind == crate::core::AggregateKind::Task
        {
            return Err(crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "task interaction binding requires an admitted journal owner",
            ));
        }
        self.interactions = Some(FilesystemInteractionHost::new(
            Arc::clone(&self.stream),
            Arc::clone(&self.host),
            verifier.audience(),
            schemas,
            crate::filesystem::FilesystemJournalBinding {
                volume: private_volume,
                verifier,
                scope,
                maximum_bytes: self.maximum_payload_bytes,
                io,
            },
        )?);
        self.invalidate_execution_caches();
        Ok(self)
    }

    /// Sets a separate finite work/byte allowance for cold canonical deltas.
    pub fn with_history_read_limits(
        mut self,
        limits: crate::store::HistoryReadLimits,
    ) -> Result<Self> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "cold history read bounds must be positive".into(),
            ));
        }
        self.history_read_limits = Some(limits);
        self.invalidate_execution_caches();
        Ok(self)
    }

    /// Binds the only selection this journal's turn may dispatch: the agent
    /// turn host's projection of the agent's own conversation stream.
    #[must_use]
    pub fn with_bound_selection(
        mut self,
        selected: crate::projection::SelectedModelContext,
    ) -> Self {
        self.bound_selection = Some(selected);
        self.invalidate_execution_caches();
        self
    }

    /// Cancels the turn's model calls and tools once `stop` fires.
    #[must_use]
    pub fn with_stop(mut self, stop: Arc<crate::agent::TurnStop>) -> Self {
        self.stop = Some(stop);
        self
    }

    /// Opens and settles approvals as files of the agent that `approvals` owns.
    #[must_use]
    pub fn with_agent_approvals(
        mut self,
        approvals: Arc<crate::approvals::AgentApprovals>,
    ) -> Self {
        self.agent_approvals = Some(approvals);
        self
    }

    /// Routes turn-input attachment admission through an explicitly bound provider set.
    #[must_use]
    pub fn with_input_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.input_verifier = Some(verifier);
        self.invalidate_execution_caches();
        self
    }

    pub(super) fn interactions(&self) -> Result<&FilesystemInteractionHost<P, A, O>> {
        self.interactions.as_ref().ok_or_else(|| {
            Error::Unsupported("task execution has no conversation interaction owner".into())
        })
    }

    #[cfg(test)]
    pub(super) fn path(&self, operation_id: OperationId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(
            Arc::clone(&self.stream),
            crate::distributed::execution_path(operation_id, &mut None)?.as_str(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    }
}
