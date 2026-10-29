use super::*;

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn invalidate_execution_caches(&mut self) {
        self.cache_identity = Default::default();
        *self.verified.get_mut() = SummaryCache::default();
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

    /// Binds the journal with optional original preparation IO.
    pub fn new(
        stream: Arc<P>,
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
        let scope = scope.copy_in_quota(&mut quota)?;
        let _native_metadata = quota.into_custody()?;
        Ok(Self {
            cache_identity: Default::default(),
            stream,
            host,
            volume,
            verifier,
            scope,
            maximum_payload_bytes,
            input_verifier: None,
            bound_selection: None,
            stop: None,
            agent_approvals: None,
            execution: None,
            verified: tokio::sync::Mutex::new(SummaryCache::default()),
            history_read_limits: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata,
        })
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
    pub fn with_stop(mut self, stop: acyclic_actors::guest::CancellationToken) -> Self {
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

    #[cfg(test)]
    pub(super) fn path(&self, operation_id: OperationId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(
            Arc::clone(&self.stream),
            crate::distributed::execution_path(operation_id, &mut None)?.as_str(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    }
}
