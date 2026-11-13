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

    /// Use the original host alarm clock for tool quiet deadlines.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn acyclic_actors::local::Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub(crate) fn with_run_admission(
        mut self,
        source: Option<crate::supervision::RunAdmissionSource>,
    ) -> Self {
        self.run_admission = source;
        self.invalidate_execution_caches();
        self
    }

    /// Publish lifecycle observations at the original journal commit. `start`
    /// selects only the first segment of the run, excluding resumed segments
    /// and self-continuations. Replays use the journal's original identity.
    pub fn with_agent_events(
        mut self,
        agent: crate::agent::AgentPath,
        run: OperationId,
        input: u64,
        start: bool,
    ) -> Self {
        self.agent_events = Some((agent, run, input, start));
        self.invalidate_execution_caches();
        self
    }

    pub(crate) fn with_actor_admission(mut self, path: Option<acyclic_stream::StreamPath>) -> Self {
        self.actor_admission = path;
        self.invalidate_execution_caches();
        self
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
        if volume.class() != VolumeClass::AgentPrivate || volume.provider() != &host.provider {
            return Err(invalid("execution journal requires a private volume"));
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            crate::executor::with_history_io(io, |work| {
                ContentGrant::authorize_volume(verifier, scope, volume, operation, work)
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
            input_verifier: None,
            bound_selection: None,
            preceding_request: None,
            stop: None,
            boundary: None,
            agent_approvals: None,
            execution: None,
            agent_events: None,
            clock: Arc::new(acyclic_actors::local::SystemClock),
            actor_admission: None,
            run_admission: None,
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

    /// Pins an original request through the existing authenticated execution index.
    /// Reuse still requires the segment writer's complete prefix-digest match.
    pub(in crate::filesystem) async fn with_preceding_turn(
        mut self,
        operation: Option<OperationId>,
    ) -> Result<Self> {
        self.preceding_request = None;
        let Some(operation) = operation else {
            return Ok(self);
        };
        let step = {
            let mut cache = self.verified.lock().await;
            self.refresh_cached_summary(operation, &mut cache, None)
                .await?;
            cache.summary().latest_response
        };
        let Some(step) = step else { return Ok(self) };
        let select = |event: &ExecutionEvent| {
            matches!(
                event,
                ExecutionEvent::ModelStarted {
                    purpose: crate::executor::ModelPurpose::Response,
                    ..
                }
            )
        };
        let (_, records) = self
            .indexed_step(
                crate::executor::ExecutionRecordQuery {
                    operation,
                    step,
                    call_id: None,
                    include_started: false,
                    maximum_step_records: crate::executor::ExecutionRecordQuery::step_bound(
                        Limits::default(),
                    ),
                    maximum: Some(1),
                    select: &select,
                },
                None,
            )
            .await?
            .ok_or_else(|| Error::Storage("preceding request index is unavailable".into()))?;
        self.preceding_request = records.into_iter().find_map(|record| match record.event {
            ExecutionEvent::ModelStarted { request, .. } => Some(request),
            _ => None,
        });
        if self.preceding_request.is_none() {
            return Err(Error::Storage(
                "preceding response has no admitted request".into(),
            ));
        }
        self.invalidate_execution_caches();
        Ok(self)
    }

    /// Cancels model calls, tools and pending platform checkpoints when `stop` fires.
    #[must_use]
    pub fn with_stop(mut self, stop: acyclic_actors::guest::CancellationToken) -> Self {
        self.verifier.bind_checkpoint_stop(&stop);
        self.stop = Some(stop);
        self
    }

    /// Binds the one file-backed inbox observer used at model boundaries.
    #[must_use]
    pub fn with_model_boundary(mut self, boundary: Arc<dyn crate::agent::ModelBoundary>) -> Self {
        self.boundary = Some(boundary);
        self.invalidate_execution_caches();
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
