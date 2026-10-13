use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
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
                    && std::ptr::eq(owner.host.as_ref(), host)
            })
    }

    /// Binds the execution to the task's exact lease and coordinator provider.
    #[allow(clippy::too_many_arguments)]
    pub fn for_task(
        owner: TaskJournalOwner<P>,
        operation_id: OperationId,
        turn_operation: OperationId,
        host: Arc<FilesystemHost<A, O>>,
        volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_payload_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        owner.validate_storage(verifier, maximum_payload_bytes, &mut work)?;
        let mut journal = Self::new(
            owner.stream(),
            host,
            volume,
            verifier,
            &SchemaRegistry::new(),
            scope,
            maximum_payload_bytes,
            io,
        )?;
        journal.owner = Some((owner, operation_id));
        journal.execution = Some((operation_id, turn_operation));
        Ok(journal)
    }

    /// Binds passive replay to an original turn without granting a task lease.
    pub(in crate::filesystem) fn with_turn(
        mut self,
        operation: OperationId,
        turn: OperationId,
    ) -> Self {
        self.execution = Some((operation, turn));
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
            .expect("original funded metadata checked")
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        schemas: &SchemaRegistry,
        scope: &Scope,
        maximum_payload_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
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
        let volume = volume.copy(&mut work)?;
        let verifier = verifier.copy(&mut work)?;
        let schemas = schemas.copy(&mut work)?;
        let scope = scope.copy(&mut work)?;
        let interactions = match verifier.audience().kind {
            crate::core::AggregateKind::Conversation => Some(FilesystemInteractionHost::new(
                stream.clone(),
                Arc::clone(&host),
                verifier.audience(),
                &verifier,
                &schemas,
                &scope,
                &volume,
                maximum_payload_bytes,
                io,
            )?),
            crate::core::AggregateKind::Task => None,
            _ => unreachable!("execution authority validated before assembly"),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = io
            .filter(|io| io.original_native_work().is_some())
            .map(|io| {
                io.take_native_custody(
                    io.owned_bytes()?
                        .checked_sub(before)
                        .ok_or_else(|| io.invalid("execution journal custody underflows"))?,
                )
            })
            .transpose()?;
        Ok(Self {
            stream,
            host,
            volume,
            verifier,
            scope,
            schemas,
            maximum_payload_bytes,
            input_verifier: None,
            interactions,
            owner: None,
            execution: None,
            verified: tokio::sync::Mutex::new(ExecutionSummary::default()),
            conversation_projection: tokio::sync::Mutex::new(None),
            history_read_limits: None,
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
            owner.require_interaction_grant(io)?;
        } else if self.verifier.audience().kind == crate::core::AggregateKind::Task {
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
            verifier,
            schemas,
            scope,
            private_volume,
            self.maximum_payload_bytes,
            io,
        )?);
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
        Ok(self)
    }

    /// Routes turn-input attachment admission through an explicitly bound provider set.
    #[must_use]
    pub fn with_input_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.input_verifier = Some(verifier);
        self
    }

    pub(super) fn interactions(&self) -> Result<&FilesystemInteractionHost<P, A, O>> {
        self.interactions.as_ref().ok_or_else(|| {
            Error::Unsupported("task execution has no conversation interaction owner".into())
        })
    }

    pub(super) fn path(&self, operation_id: OperationId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(
            Arc::clone(&self.stream),
            crate::distributed::execution_path(operation_id, &mut None)?.as_str(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    }
}
