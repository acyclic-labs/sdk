use super::*;

/// One logical coordinator whose entire semantic history is a Stream.
pub struct DistributedCoordinator<P> {
    pub(crate) client: std::sync::Arc<P>,
    pub(super) stream: (
        acyclic_stream::Stream<P>,
        Option<crate::runtime::NativeCustody>,
    ),
    pub(super) scheduler: Scheduler,
    pub(super) revision: u64,
    pub(super) last_committed_at_ms: u64,
    pub(super) content_verifier: Arc<dyn ContentResidencyVerifier>,
    pub(super) payload_store: Option<Arc<dyn SchedulerPayloadStore>>,
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    /// Opens and replays the coordinator history.
    pub async fn open(
        client: &std::sync::Arc<P>,
        content_verifier: Arc<dyn ContentResidencyVerifier>,
        payload_store: Option<Arc<dyn SchedulerPayloadStore>>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let stream = coordinator_stream(client, work)?;
        let mut value = Self {
            client: client.clone(),
            stream,
            scheduler: Scheduler::new(),
            revision: 0,
            last_committed_at_ms: 0,
            content_verifier,
            payload_store,
        };
        value.refresh(work).await?;
        Ok(value)
    }

    /// Replays commits made by other coordinator instances from the exact observed revision.
    pub async fn refresh(&mut self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        loop {
            let mut page_custody = CodecQuota::new(work, 0)?;
            let from = self.revision;
            let mut next = from;
            let mut source = read_records(
                &self.client,
                &self.stream.0,
                from,
                READ_PAGE_SIZE,
                page_custody.work(),
            )
            .await?;
            while let Some(record) = next_record(
                &mut source,
                &mut next,
                from.saturating_add(u64::from(READ_PAGE_SIZE)),
                &mut page_custody,
            )
            .await?
            {
                let raw_bytes = operations::sdk_record_retained_bytes(&record, page_custody.work());
                let mut event_custody = CodecQuota::new(page_custody.work(), 0)?;
                if let Some(work) = event_custody.work().as_deref_mut() {
                    work.check_cancellation()?;
                }
                let (revision, key, _, committed_at_ms, event) =
                    decode(&record, event_custody.work())?;
                if event_custody.work().is_some() {
                    event_custody.adopt(event.logical_owned_bytes()?)?;
                    event_custody.adopt(key.capacity() as u64)?;
                }
                if let SchedulerEvent::Declared { spec } = &event {
                    self.verify_content(&spec.state, event_custody.work())
                        .await?;
                }
                self.verify_published_result(&event, event_custody.work())
                    .await?;
                if let Some((_, existing, ..)) = read_intent(
                    &self.client,
                    &key,
                    Some(self.revision),
                    event_custody.work(),
                )
                .await?
                {
                    if event_custody.work().is_some() {
                        event_custody.adopt(existing.logical_owned_bytes()?)?;
                    }
                    return Err(coordinator_boundary(
                        event_custody.work(),
                        wire::ErrorCode::Conflict,
                        "coordinator history repeats an idempotency key",
                    ));
                }
                if let Some(work) = event_custody.work().as_deref_mut() {
                    work.check_cancellation()?;
                }
                if committed_at_ms <= self.last_committed_at_ms {
                    return Err(coordinator_boundary(
                        event_custody.work(),
                        wire::ErrorCode::Storage,
                        "coordinator commit time is not increasing",
                    ));
                }
                self.scheduler.apply(&event, event_custody.work())?;
                self.revision = revision;
                self.last_committed_at_ms = committed_at_ms;
                drop((key, event));
                drop(event_custody);
                drop(record);
                if page_custody.work().is_some() {
                    page_custody.relinquish(raw_bytes)?;
                }
            }
            if next == from {
                break;
            }
        }
        Ok(())
    }

    async fn intent_matches(
        &self,
        key: &str,
        event: &SchedulerEvent,
        digest: &[u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<bool>> {
        let mut custody = CodecQuota::new(work, 0)?;
        let Some(intent) =
            read_intent(&self.client, key, Some(self.revision), custody.work()).await?
        else {
            return Ok(None);
        };
        if custody.work().is_some() {
            custody.adopt(intent.1.logical_owned_bytes()?)?;
        }
        Ok(Some(&intent.0 == digest && &intent.1 == event))
    }

    pub(super) async fn verify_content(
        &self,
        reference: &FileRef,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        match work.as_deref_mut() {
            Some(work) => {
                crate::store::history::verify_content_with_work(
                    work,
                    self.content_verifier.as_ref(),
                    reference,
                )
                .await
            }
            None => {
                self.content_verifier
                    .verify(reference, reference.descriptor().byte_length(), None)?
                    .await
            }
        }
    }

    async fn verify_published_result(
        &self,
        event: &SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let SchedulerEvent::WorkflowResumed { input, .. } = event {
            return self.verify_content(input, work).await;
        }
        let (operation_id, outcome, reducer) = match event {
            SchedulerEvent::Completed {
                operation_id,
                outcome,
                ..
            } => (*operation_id, outcome, None),
            SchedulerEvent::Orchestrated {
                operation_id,
                outcome,
                reducer,
                ..
            } => (*operation_id, outcome, reducer.as_ref()),
            _ => return Ok(()),
        };
        let crate::Outcome::Succeeded(reference) = outcome else {
            return Ok(());
        };
        let mut custody = CodecQuota::new(work, 0)?;
        let (value, value_bytes) = self.load_json(reference, custody.work()).await?;
        custody.adopt(value_bytes)?;
        if let SchedulerEvent::Orchestrated {
            operation_id,
            reducer: None,
            ..
        } = event
        {
            let decision = self
                .scheduler
                .orchestration(*operation_id, custody.work())?;
            let decision_bytes = if custody.work().is_some() {
                decision.logical_owned_bytes()?
            } else {
                0
            };
            custody.adopt(decision_bytes)?;
            if let OrchestrationDecision::Assemble {
                assembly, values, ..
            } = &decision
            {
                let (expected, expected_bytes) = self
                    .assemble_values(*assembly, values, custody.work())
                    .await?;
                custody.adopt(expected_bytes)?;
                let differs = value != expected;
                drop(expected);
                custody.relinquish(expected_bytes)?;
                if differs {
                    return Err(coordinator_boundary(
                        custody.work(),
                        wire::ErrorCode::Invalid,
                        "assembled scheduler result differs from pinned children",
                    ));
                }
            }
            drop(decision);
            custody.relinquish(decision_bytes)?;
        }
        let operation = self.scheduler.operation(operation_id).ok_or_else(|| {
            coordinator_boundary(
                custody.work(),
                wire::ErrorCode::NotFound,
                "scheduler result operation is absent",
            )
        })?;
        for schema in [
            Some(&operation.spec.entrypoint.result_schema),
            reducer.map(|item| &item.result_schema),
        ]
        .into_iter()
        .flatten()
        {
            crate::contract::validate_json_schema_value_with_work(
                schema,
                &value,
                "scheduler result",
                custody.work(),
            )?;
        }
        Ok(())
    }

    /// Returns the deterministic scheduler projection.
    #[must_use]
    pub const fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    #[cfg(any(feature = "filesystem", test))]
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    /// Applies an event and returns whether this call appended it.
    pub async fn apply(
        &mut self,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        if matches!(
            &event,
            SchedulerEvent::Declared { .. }
                | SchedulerEvent::SessionConfigured { .. }
                | SchedulerEvent::WorkflowResumed { .. }
                | SchedulerEvent::Orchestrated { .. }
        ) {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Unauthorized,
                "scheduler event requires its scoped coordinator API",
            ));
        }
        self.apply_internal(idempotency_key, event, None, None, None, None, None, work)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_internal(
        &mut self,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
        parent_fence: Option<&LeaseFence>,
        waiting_command: Option<OperationId>,
        idle_execution: Option<(OperationId, u64)>,
        workflow_guard: Option<(&str, u64)>,
        verified_revision: Option<u64>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        self.refresh(work).await?;
        if verified_revision.is_some_and(|revision| revision != self.revision) {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Conflict,
                "coordinator changed after owner verification",
            ));
        }
        self.validate_parent_owner(&event, parent_fence, work)?;
        validate_idempotency_key(idempotency_key.as_str(), work)?;
        let key = idempotency_key.as_str();
        let operation_id = event.operation_id();
        let mut custody = CodecQuota::new(work, 0)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &event,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            custody.work(),
        )?;
        if custody.work().is_some() {
            custody.adopt(canonical.capacity() as u64)?;
        }
        if let Some(work) = custody.work().as_deref_mut() {
            work.hashed_bytes(canonical.len() as u64)?;
        }
        let digest = *blake3::hash(&canonical).as_bytes();
        if let Some(matches) = self
            .intent_matches(key, &event, &digest, custody.work())
            .await?
        {
            return if matches {
                self.validate_command_wait(&event, waiting_command, true, custody.work())?;
                self.validate_replay(&event, custody.work()).map(|()| false)
            } else {
                Err(coordinator_boundary(
                    custody.work(),
                    wire::ErrorCode::Conflict,
                    "coordinator retry identity reused",
                ))
            };
        }
        self.validate_command_wait(&event, waiting_command, false, custody.work())?;
        if let SchedulerEvent::Declared { spec } = &event {
            self.verify_content(&spec.state, custody.work()).await?;
        }
        self.verify_published_result(&event, custody.work()).await?;
        self.scheduler.validate_event(&event, custody.work())?;
        let revision = next_revision(self.revision)?;
        let committed_at_ms = self.next_commit_time(custody.work())?;
        custody.transfer();
        let bytes = encode(
            revision,
            operation_id,
            key,
            digest,
            canonical,
            committed_at_ms,
            custody.work(),
        )?;
        let (record, retained) = operations::owned_bytes_from_retained_vec(bytes, custody.work())?;
        custody.adopt(retained)?;
        let current_revision = self.revision;
        let digits = |value: u64| {
            value
                .checked_ilog10()
                .map_or(1, |digits| digits as usize + 1)
        };
        let (stream_key, retained) = if parent_fence.is_some() {
            stream_key(
                key.len() + ":owned-parent:".len() + digits(current_revision),
                format_args!("{key}:owned-parent:{current_revision}"),
                custody.work(),
            )
        } else if waiting_command.is_some() {
            stream_key(
                key.len() + ":owned-wait:".len() + digits(current_revision),
                format_args!("{key}:owned-wait:{current_revision}"),
                custody.work(),
            )
        } else if let Some((execution, tail)) = idle_execution {
            stream_key(
                key.len()
                    + ":idle-execution:".len()
                    + 36
                    + 2
                    + digits(tail)
                    + digits(current_revision),
                format_args!("{key}:idle-execution:{execution}:{tail}:{current_revision}"),
                custody.work(),
            )
        } else {
            stream_key(key.len(), format_args!("{key}"), custody.work())
        }?;
        custody.adopt(retained)?;
        let appended = if let Some((execution, expected_tail)) = idle_execution {
            append_if_execution_idle(
                &self.client,
                &self.stream.0,
                record,
                self.revision,
                stream_key,
                operation_id,
                execution,
                expected_tail,
                workflow_guard,
                custody.work(),
            )
            .await
        } else {
            append(
                (&self.stream.0, &self.client),
                [record],
                0,
                Some(self.revision),
                stream_key,
                operation_id,
                custody.work(),
            )
            .await
        };
        custody.relinquish(custody.retained())?;
        let outcome = match appended {
            Ok(outcome) => outcome,
            Err(error) if idle_execution.is_none() && coordinator_retry_conflict(&error) => {
                self.refresh(custody.work()).await?;
                if self
                    .intent_matches(key, &event, &digest, custody.work())
                    .await?
                    == Some(true)
                {
                    self.validate_command_wait(&event, waiting_command, true, custody.work())?;
                    self.validate_parent_owner(&event, parent_fence, custody.work())?;
                    return self.validate_replay(&event, custody.work()).map(|()| false);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let applied = match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(1) || receipt.tail < receipt.end {
                    return Err(coordinator_boundary(
                        custody.work(),
                        wire::ErrorCode::Storage,
                        "invalid coordinator append receipt",
                    ));
                }
                receipt.start == revision - 1
            }
            AppendOutcome::TailConflict { .. } => false,
        };
        self.refresh(custody.work()).await?;
        if self
            .intent_matches(key, &event, &digest, custody.work())
            .await?
            != Some(true)
        {
            return Err(coordinator_boundary(
                custody.work(),
                wire::ErrorCode::Conflict,
                "coordinator publication differs from its intent",
            ));
        }
        self.validate_replay(&event, custody.work())?;
        self.validate_parent_owner(&event, parent_fence, custody.work())?;
        Ok(applied)
    }

    fn validate_replay(
        &self,
        event: &SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let SchedulerEvent::ModelDispatchClaimed {
            operation_id,
            fence,
            ..
        } = event
        {
            let operation = self.scheduler.operation(*operation_id).ok_or_else(|| {
                coordinator_boundary(work, wire::ErrorCode::NotFound, "model task")
            })?;
            crate::scheduler::require_execution_owner(operation, fence, false, work)?;
        }
        Ok(())
    }

    fn validate_command_wait(
        &self,
        event: &SchedulerEvent,
        waiting: Option<OperationId>,
        replaying: bool,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let Some(waiting) = waiting else {
            return Ok(());
        };
        let SchedulerEvent::WorkflowResumed {
            operation_id,
            workflow_revision,
            input,
        } = event
        else {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Invalid,
                "command wait guard only applies to wake",
            ));
        };
        let operation = self
            .scheduler
            .operation(*operation_id)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound, "waiting task"))?;
        if (!replaying && operation.phase != crate::scheduler::OperationPhase::Suspended)
            || operation.phase == crate::scheduler::OperationPhase::Terminal
            || operation.cancellation_requested
            || operation.workflow.as_ref().is_none_or(|slot| {
                slot.revision != *workflow_revision
                    || slot.waiting_command != Some(waiting)
                    || slot.input.as_ref() != replaying.then_some(input)
            })
        {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Conflict,
                "selected command wait is no longer current",
            ));
        }
        Ok(())
    }

    fn validate_parent_owner(
        &self,
        event: &SchedulerEvent,
        fence: Option<&LeaseFence>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let Some(fence) = fence else {
            return Ok(());
        };
        let parent = match event {
            SchedulerEvent::Declared { spec } => {
                spec.parent
                    .as_ref()
                    .ok_or_else(|| {
                        coordinator_boundary(
                            work,
                            wire::ErrorCode::Invalid,
                            "child declaration has no parent",
                        )
                    })?
                    .operation_id
            }
            SchedulerEvent::CancellationRequested {
                operation_id,
                recursive: true,
            } => {
                self.scheduler
                    .operation(*operation_id)
                    .ok_or_else(|| {
                        coordinator_boundary(work, wire::ErrorCode::NotFound, "cancellation child")
                    })?
                    .spec
                    .parent
                    .as_ref()
                    .ok_or_else(|| {
                        coordinator_boundary(
                            work,
                            wire::ErrorCode::Unauthorized,
                            "cancellation requires an authorized direct child",
                        )
                    })?
                    .operation_id
            }
            _ => {
                return Err(coordinator_boundary(
                    work,
                    wire::ErrorCode::Invalid,
                    "parent fence only applies to declaration or recursive child cancellation",
                ));
            }
        };
        let operation = self.scheduler.operation(parent).ok_or_else(|| {
            coordinator_boundary(work, wire::ErrorCode::NotFound, "admitting parent")
        })?;
        crate::scheduler::require_execution_owner(operation, fence, false, work)
    }

    pub(super) fn next_commit_time(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<u64> {
        let after_previous = self.last_committed_at_ms.checked_add(1).ok_or_else(|| {
            coordinator_boundary(
                work,
                wire::ErrorCode::Invalid,
                "coordinator commit time exhausted",
            )
        })?;
        use acyclic_stream::UnixMillisClock as _;
        let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
        if now == 0 || now == u64::MAX {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Invalid,
                "coordinator clock exceeds supported range",
            ));
        }
        Ok(now.max(after_previous))
    }
    pub(crate) async fn apply_if_execution_quiescent(
        &mut self,
        key: IdempotencyKey,
        event: SchedulerEvent,
        execution: OperationId,
        expected_tail: u64,
        workflow_guard: Option<(&str, u64)>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let verified_revision = work.is_some().then_some(self.revision);
        if !matches!(
            &event,
            SchedulerEvent::WorkflowSuspended { .. } | SchedulerEvent::LeaseReleased { .. }
        ) {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Invalid,
                "idle execution guard requires suspension or lease release",
            ));
        }
        if work.is_some()
            && matches!(&event, SchedulerEvent::LeaseReleased { .. })
            && workflow_guard.is_none()
        {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Invalid,
                "measured lease release requires its original workflow witness",
            ));
        }
        self.apply_internal(
            key,
            event,
            None,
            None,
            Some((execution, expected_tail)),
            workflow_guard,
            verified_revision,
            work,
        )
        .await
    }
}
