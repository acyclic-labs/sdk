use super::*;
/// Executes a pinned dispatch once and reconciles its journal.
pub struct DurableToolRunner<'a> {
    tools: &'a ToolRegistry,
    journal: Arc<dyn ExecutionJournal>,
    limits: Limits,
    work: NativeWork<'a>,
}
impl<'tools> DurableToolRunner<'tools> {
    /// Pins the borrowed registry and owner-bound journal.
    pub fn new(
        tools: &'tools ToolRegistry,
        journal: Arc<dyn ExecutionJournal>,
        work: NativeWork<'tools>,
    ) -> Result<Self> {
        let io = preparation_io(work)?;
        let _ = &io;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work
            && !journal.supports_native_execution(work)?
        {
            return Err(boundary(
                io.as_ref(),
                Unsupported,
                "journal has no native producer",
            ));
        }
        Ok(Self {
            tools,
            journal,
            limits: Limits::default(),
            work,
        })
    }
    /// Sets finite publication limits.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        let io = preparation_io(self.work)?;
        with_history_io(io.as_ref(), |work| limits.validate(work))?;
        self.limits = limits;
        Ok(self)
    }
    /// Runs or reconciles the admitted invocation on its original work.
    pub fn run<'a>(
        &'a self,
        task_id: TaskId,
        definition: &'a ToolDefinition,
        invocation: &'a ToolInvocation,
        context: &'a ToolContext,
        work: NativeWork<'a>,
        reconcile_only: bool,
    ) -> Result<crate::store::HistoryContentFuture<'a, (Outcome<Value>, NativeCustody)>> {
        let io = preparation_io(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work
            && (!context.native_metadata_matches(work)
                || !self
                    .work
                    .is_some_and(|original| original.shares_ledger(work)))
        {
            return Err(boundary(
                io.as_ref(),
                Unauthorized,
                "tool context or runner belongs to another work",
            ));
        }
        let tool = self
            .tools
            .get(&definition.name, Some(&definition.revision), io.as_ref())?
            .ok_or_else(|| {
                boundary(
                    io.as_ref(),
                    NotFound,
                    "admitted tool revision is not registered",
                )
            })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            tool.executor.supports_native_execution(work)?;
            tool.projection.supports_native_projection(Some(work))?;
        }
        let future = || async move {
            self.run_dispatch(
                tool,
                (task_id, definition, invocation, context),
                reconcile_only,
                io.as_ref(),
            )
            .await
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            return crate::NativeFutureBundle::new(work, future)
                .map(crate::store::HistoryContentFuture::Native);
        }
        Ok(crate::store::HistoryContentFuture::Ordinary {
            future: Box::pin(future()),
            frame_bytes: 0,
        })
    }
    async fn run_dispatch(
        &self,
        tool: &Tool,
        binding: (TaskId, &ToolDefinition, &ToolInvocation, &ToolContext),
        reconcile_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<(Outcome<Value>, NativeCustody)> {
        let (task_id, definition, invocation, context) = binding;

        let operation_id = invocation.operation_id;
        let (limits, digest) =
            self.validate_dispatch(task_id, definition, invocation, context, tool, io)?;
        loop {
            // Reauthorize before reading or publishing replay.
            authorize(tool, invocation, context, io).await?;
            let (_, records, hold) =
                replay_execution(self.journal.as_ref(), operation_id, 4, |_| true, io).await?;
            let records = (records, hold);
            self.validate_dispatch_history(&records.0, invocation, digest, limits, io)
                .await?;
            if let Some(ExecutionEvent::ToolCompleted {
                result, projection, ..
            }) = records.0.get(2).map(|record| &record.event)
            {
                return self
                    .replay_tool_completion(definition, result, projection, limits, io)
                    .await;
            }
            if let Some(ExecutionEvent::ToolFailed { reason, .. }) =
                records.0.get(2).map(|record| &record.event)
            {
                return tool_outcome(operation_id, Some(*reason), io);
            }
            let started = !records.0.is_empty();
            let dispatched = records.0.len() >= 2;
            if reconcile_only && !dispatched {
                return tool_outcome(operation_id, None, io);
            }
            drop(records);
            if !started {
                self.journal
                    .append(
                        operation_id,
                        "tool:started",
                        &ExecutionEvent::Started {
                            request_digest: digest,
                        },
                        io,
                    )?
                    .await?;
            }
            let claimed = if dispatched {
                false
            } else {
                match self.claim_dispatch(invocation, digest, io).await? {
                    Some(true) => true,
                    Some(false) => continue,
                    None => return tool_outcome(operation_id, None, io),
                }
            };
            macro_rules! fail {
                ($reason:expr) => {
                    if let Some(outcome) = self.fail(operation_id, invocation, $reason, io).await? {
                        return Ok(outcome);
                    }
                    continue;
                };
            }
            let execution = if claimed {
                self.invoke(tool, invocation, context, io).await.map(Some)
            } else {
                self.reconcile(tool, invocation, context, io).await
            };
            let result = match execution {
                Ok(Some(result)) => result,
                Ok(None) => return tool_outcome(operation_id, None, io),
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(error) if error.leaves_effect_uncertain() => {
                    return tool_outcome(operation_id, None, io);
                }
                Err(error) => {
                    if !claimed {
                        return Err(error);
                    }
                    fail!(ToolFailureKind::ExecutorRejected);
                }
            };
            let projection =
                match self.project_output(definition, invocation, tool, &result.0, limits, io)? {
                    Ok(projection) => projection,
                    Err(reason) => {
                        fail!(reason);
                    }
                };
            if let Some(outcome) = self
                .publish_output(invocation, result, &projection.0, io)
                .await?
            {
                return Ok(outcome);
            }
        }
    }

    async fn publish_output(
        &self,
        invocation: &ToolInvocation,
        result: (ToolResult, NativeCustody),
        projection: &Value,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(Outcome<Value>, NativeCustody)>> {
        let operation_id = invocation.operation_id;
        let result_ref = match self.stage(operation_id, "tool:result", &result.0, io).await {
            Ok(reference) => reference,
            Err(error) if tool_propagate(&error, io) => return Err(error),
            Err(error) if error.leaves_effect_uncertain() => {
                return tool_outcome(operation_id, None, io).map(Some);
            }
            Err(_) => {
                return self
                    .fail(
                        operation_id,
                        invocation,
                        ToolFailureKind::PublicationRejected,
                        io,
                    )
                    .await;
            }
        };
        let projection_ref = match self
            .stage(operation_id, "tool:projection", projection, io)
            .await
        {
            Ok(reference) => reference,
            Err(error) if tool_propagate(&error, io) => return Err(error),
            Err(error) if error.leaves_effect_uncertain() => {
                return tool_outcome(operation_id, None, io).map(Some);
            }
            Err(_) => {
                return self
                    .fail(
                        operation_id,
                        invocation,
                        ToolFailureKind::PublicationRejected,
                        io,
                    )
                    .await;
            }
        };
        let key = tool_text(format_args!("tool:completed:{}", OperationId::new()), io)?;
        let call = tool_text(format_args!("{}", invocation.call_id), io)?;
        let event = ExecutionEvent::ToolCompleted {
            step: 0,
            call_id: call.0,
            result: result_ref.0,
            projection: projection_ref.0,
        };
        match match self
            .journal
            .append_if_tail(operation_id, 2, &key.0, &event, io)
        {
            Ok(future) => future.await,
            Err(error) => Err(error),
        } {
            Ok(true) => tool_success(result, io).map(Some),
            Ok(false) => Ok(None),
            Err(error) if tool_propagate(&error, io) => Err(error),
            Err(error) if error.leaves_effect_uncertain() => {
                tool_outcome(operation_id, None, io).map(Some)
            }
            Err(error) => Err(error),
        }
    }

    fn validate_dispatch(
        &self,
        task_id: TaskId,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        context: &ToolContext,
        tool: &Tool,
        io: Option<&PreparationIo>,
    ) -> Result<(Limits, [u8; 32])> {
        let operation_id = invocation.operation_id;
        if tool.definition != *definition || invocation.name != definition.name {
            return Err(boundary(io, Conflict, "admitted tool definition changed"));
        }
        if context.operation_id() != operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id() != Some(task_id)
        {
            return Err(boundary(
                io,
                Unauthorized,
                "tool invocation context mismatch",
            ));
        }
        let scope = context.effective_scope();
        let limits = Limits {
            file_bytes: self.limits.file_bytes.min(scope.limits().file_bytes),
            render_bytes: self.limits.render_bytes.min(scope.limits().render_bytes),
            path_bytes: self.limits.path_bytes.min(scope.limits().path_bytes),
            attachments: self.limits.attachments.min(scope.limits().attachments),
            ..self.limits
        };
        invocation.validate_recorded(&Value::Null, limits, io)?;
        let digest = preparation_digest(
            &("acyclic.tool.v2", &task_id, definition, invocation, limits),
            io,
        )?;
        Ok((limits, digest))
    }

    async fn validate_dispatch_history(
        &self,
        records: &[crate::executor::ExecutionRecord],
        invocation: &ToolInvocation,
        digest: [u8; 32],
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        for (index, record) in records.iter().enumerate() {
            match (index, &record.event) {
                (0, ExecutionEvent::Started { request_digest }) if *request_digest == digest => {}
                (
                    1,
                    ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id,
                        invocation: reference,
                    },
                ) if *call_id == invocation.call_id => {
                    if reference.descriptor().byte_length() > limits.file_bytes {
                        return Err(boundary(
                            io,
                            Conflict,
                            "invocation descriptor exceeds limit",
                        ));
                    }
                    let pinned = self
                        .load::<crate::tool::RecordedInvocation>(reference, io)
                        .await?;
                    if pinned.0.invocation != *invocation || !pinned.0.metadata.is_null() {
                        return Err(boundary(io, Conflict, "journal invocation changed"));
                    }
                }
                (
                    2,
                    ExecutionEvent::ToolCompleted {
                        step: 0, call_id, ..
                    }
                    | ExecutionEvent::ToolFailed {
                        step: 0, call_id, ..
                    },
                ) if *call_id == invocation.call_id => {}
                _ => return Err(boundary(io, Conflict, "incompatible tool history")),
            }
        }
        Ok(())
    }
    async fn replay_tool_completion(
        &self,
        definition: &ToolDefinition,
        result: &FileRef,
        projection: &FileRef,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<(Outcome<Value>, NativeCustody)> {
        if result.descriptor().byte_length() > limits.file_bytes
            || projection.descriptor().byte_length() > limits.render_bytes
        {
            return Err(boundary(
                io,
                Conflict,
                "tool history descriptor exceeds limits",
            ));
        }
        let result = self.load::<ToolResult>(result, io).await?;
        let projection = self.load::<Value>(projection, io).await?;
        if let Err(error) = tool_json_bound(&result.0, limits.file_bytes, io)
            .and_then(|()| tool_json_bound(&projection.0, limits.render_bytes, io))
        {
            if tool_propagate(&error, io) {
                return Err(error);
            }
            return Err(boundary(io, Conflict, "tool history exceeds limits"));
        }
        with_history_io(io, |work| {
            validate_json_schema_value(
                &definition.output_schema,
                &result.0.value,
                "tool output",
                work,
            )
        })?;
        tool_validate_projection(definition, &projection.0, limits, io)?;
        tool_success(result, io)
    }

    async fn claim_dispatch(
        &self,
        invocation: &ToolInvocation,
        digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<Option<bool>> {
        let operation_id = invocation.operation_id;
        let claimed;
        {
            let record = crate::tool::RecordedInvocation {
                invocation,
                metadata: &Value::Null,
            };
            let reference = self
                .stage(operation_id, "tool:invocation", &record, io)
                .await?;
            let (_, current, hold) =
                replay_execution(self.journal.as_ref(), operation_id, 4, |_| true, io).await?;
            let current = (current, hold);
            if matches!(current.0.as_slice(), [record] if matches!(&record.event, ExecutionEvent::Started { request_digest } if *request_digest == digest))
            {
                let key = tool_text(format_args!("tool:claim:{}", OperationId::new()), io)?;
                let call = tool_text(format_args!("{}", invocation.call_id), io)?;
                let event = ExecutionEvent::ToolStarted {
                    step: 0,
                    call_id: call.0,
                    invocation: reference.0,
                };
                match match self
                    .journal
                    .append_if_tail(operation_id, 1, &key.0, &event, io)
                {
                    Ok(future) => future.await,
                    Err(error) => Err(error),
                } {
                    Ok(true) => claimed = true,
                    Ok(false) => return Ok(Some(false)),
                    Err(error) if tool_propagate(&error, io) => return Err(error),
                    Err(Error::Indeterminate(_)) => {
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
            } else if !matches!(current.0.get(1).map(|record| &record.event), Some(ExecutionEvent::ToolStarted { step: 0, call_id, .. }) if *call_id == invocation.call_id)
            {
                return Err(boundary(io, Conflict, "journal changed before dispatch"));
            } else {
                return Ok(Some(false));
            }
        }
        Ok(Some(claimed))
    }

    fn project_output(
        &self,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        tool: &Tool,
        result: &ToolResult,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<std::result::Result<(Value, NativeCustody), ToolFailureKind>> {
        if let Err(error) = with_history_io(io, |work| {
            validate_json_schema_value(
                &definition.output_schema,
                &result.value,
                "tool output",
                work,
            )
        }) {
            if tool_propagate(&error, io) {
                return Err(error);
            }
            return Ok(Err(ToolFailureKind::InvalidOutput));
        }
        if let Err(error) = tool_json_bound(result, limits.file_bytes, io) {
            if tool_propagate(&error, io) {
                return Err(error);
            }
            return Ok(Err(ToolFailureKind::PublicationRejected));
        }
        let projection = match tool.projection.project(invocation, result, native_work(io)) {
            Ok(projection) => projection,
            Err(error) if tool_propagate(&error, io) => return Err(error),
            Err(_) => {
                return Ok(Err(ToolFailureKind::ProjectionRejected));
            }
        };
        if native_work(io).is_some() && projection.1.is_none() {
            return Err(boundary(io, Unsupported, "tool projection has no custody"));
        }
        if let Err(error) = tool_validate_projection(definition, &projection.0, limits, io)
            .and_then(|()| tool_json_bound(&projection.0, limits.render_bytes, io))
        {
            if tool_propagate(&error, io) {
                return Err(error);
            }
            return Ok(Err(ToolFailureKind::ProjectionRejected));
        }
        Ok(Ok(projection))
    }
}
mod journal;
use journal::authorize;
mod value;
use value::*;
