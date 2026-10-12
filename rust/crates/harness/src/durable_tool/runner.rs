use super::*;

/// Executes only after a durable dispatch boundary; replay observes or reconciles that boundary and never calls `execute` a second time.
pub struct DurableToolRunner {
    tools: ToolRegistry,
    journal: Arc<dyn ExecutionJournal>,
    limits: Limits,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl DurableToolRunner {
    /// Pins the registered definitions and the owner-bound ref-only journal.
    pub fn new(
        tools: &ToolRegistry,
        journal: Arc<dyn ExecutionJournal>,
        _work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _work {
            if !journal.supports_native_execution(work)? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "tool journal has no native execution producer",
                });
            }
            let metadata =
                (std::mem::size_of::<Self>() - std::mem::size_of::<ToolRegistry>()) as u64;
            let mut custody = work.claim(metadata, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
            let mut tools_custody;
            let (tools, hold) = tools.clone_with_native_work(work)?;
            tools_custody = hold;
            custody.absorb(&mut tools_custody)?;
            return Ok(Self {
                tools,
                journal,
                limits: Limits::default(),
                native_custody: Some(custody),
            });
        }
        Ok(Self {
            tools: tools.clone(),
            journal,
            limits: Limits::default(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        })
    }

    /// Narrows the maximum artifact and projection bytes before publication.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &self.native_custody {
            limits.validate(&mut Some(&mut crate::store::history::native_work_io(
                custody.work(),
            )?))?;
            self.limits = limits;
            return Ok(self);
        }
        limits.validate(&mut None)?;
        self.limits = limits;
        Ok(self)
    }

    /// Executes with the already admitted typed context supplied by a durable task host.
    pub async fn run_with_context(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: ToolDefinition,
        arguments: Value,
        context: ToolContext,
    ) -> Result<Outcome<Value>> {
        let tool = self
            .tools
            .get_version(&definition.name, &definition.revision, None)?
            .ok_or_else(|| {
                Error::NotFound(format!("tool {}@{}", definition.name, definition.revision))
            })?;
        let invocation = (
            ToolInvocation {
                operation_id,
                call_id: operation_id.to_string(),
                name: definition.name.clone(),
                arguments,
            },
            None,
        );
        Ok(self
            .run_engine(
                task_id,
                operation_id,
                &definition,
                invocation,
                &context,
                tool,
                None,
                false,
            )
            .await?
            .0)
    }

    /// Runs the canonical dispatch/reconciliation engine on the original work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn run_with_native_context<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &'a ToolDefinition,
        arguments: &'a Value,
        context: &'a ToolContext,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Outcome<Value>, acyclic_fs::NativeAllocation)>> {
        self.native_run(
            task_id,
            operation_id,
            definition,
            arguments,
            context,
            work,
            false,
        )
    }

    /// Only observes a retained dispatch proof. Missing intent never dispatches.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn reconcile_with_native_context<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &'a ToolDefinition,
        arguments: &'a Value,
        context: &'a ToolContext,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Outcome<Value>, acyclic_fs::NativeAllocation)>> {
        self.native_run(
            task_id,
            operation_id,
            definition,
            arguments,
            context,
            work,
            true,
        )
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments, reason = "original admitted tool envelope")]
    fn native_run<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &'a ToolDefinition,
        arguments: &'a Value,
        context: &'a ToolContext,
        work: &'a acyclic_fs::NativeMaterializationWork,
        reconcile_only: bool,
    ) -> Result<crate::NativeFutureBundle<'a, (Outcome<Value>, acyclic_fs::NativeAllocation)>> {
        work.remaining()?;
        if !context.native_metadata_matches(work)
            || !self
                .native_custody
                .as_ref()
                .is_some_and(|guard| guard.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool context or runner metadata has no original custody",
            });
        }
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let tool = self
            .tools
            .get_version(&definition.name, &definition.revision, Some(&io))?
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::NotFound,
                reason: "admitted tool revision is not registered",
            })?;
        tool.executor.supports_native_execution(work)?;
        tool.projection.supports_native_projection(Some(work))?;
        if !self.journal.supports_native_execution(work)? {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "tool journal has no native execution producer",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let mut custody = work.claim(
                (std::mem::size_of::<ToolInvocation>() - std::mem::size_of::<Value>()) as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let mut call_hold;
            let (call_id, hold) = work.display(&context.call_id())?;
            call_hold = hold;
            custody.absorb(&mut call_hold)?;
            let mut name_hold;
            let (name, hold) = work.display(&definition.name)?;
            name_hold = hold;
            custody.absorb(&mut name_hold)?;
            let mut arguments_hold;
            let (arguments, hold) = crate::tool::clone_value(arguments, None, work)?;
            arguments_hold = hold;
            custody.absorb(&mut arguments_hold)?;
            let invocation = (
                ToolInvocation {
                    operation_id,
                    call_id,
                    name,
                    arguments,
                },
                Some(custody),
            );
            let result = self
                .run_engine(
                    task_id,
                    operation_id,
                    definition,
                    invocation,
                    context,
                    tool,
                    Some(&io),
                    reconcile_only,
                )
                .await?;
            let guard = result.1.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "native tool outcome has no custody",
            })?;
            Ok((result.0, guard))
        })
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one canonical ordered durable tool state machine"
    )]
    async fn run_engine(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &ToolDefinition,
        invocation: (ToolInvocation, crate::NativeCustody),
        context: &ToolContext,
        tool: &crate::tool::Tool,
        io: Option<&crate::executor::PreparationIo>,
        reconcile_only: bool,
    ) -> Result<(Outcome<Value>, crate::NativeCustody)> {
        let invocation = &invocation.0;
        if tool.definition != *definition {
            return Err(tool_error(
                io,
                crate::wire::ErrorCode::Conflict,
                "durable tool definition changed after admission",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
        })?;
        if context.operation_id() != operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id() != Some(task_id)
        {
            return Err(tool_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "tool context is not bound to this admitted effect",
            ));
        }
        let scope = context.effective_scope();
        let content_limit = self.limits.file_bytes.min(scope.limits().file_bytes);
        let projection_limit = self.limits.render_bytes.min(scope.limits().render_bytes);
        let projection_limits = Limits {
            file_bytes: content_limit,
            render_bytes: projection_limit,
            path_bytes: self.limits.path_bytes.min(scope.limits().path_bytes),
            attachments: self.limits.attachments.min(scope.limits().attachments),
            ..self.limits
        };
        tool_json_bound(invocation, content_limit, io)?;
        let digest = crate::executor::preparation_digest(
            &(&task_id, definition, invocation, projection_limits),
            io,
        )?;
        loop {
            // Authentication is repeated BEFORE reading or publishing replay.
            self.authorize(tool, definition, invocation, context, io)
                .await?;
            let records = self.replay(operation_id, io).await?;
            let mut started = false;
            let mut dispatched = false;
            let mut completed = None;
            let mut failed = None;
            for record in &records.0 {
                match &record.event {
                    ExecutionEvent::Started { request_digest }
                        if !started
                            && !dispatched
                            && completed.is_none()
                            && failed.is_none()
                            && *request_digest == digest =>
                    {
                        started = true
                    }
                    ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id,
                        invocation: reference,
                    } if started
                        && !dispatched
                        && failed.is_none()
                        && *call_id == invocation.call_id =>
                    {
                        if reference.descriptor().byte_length() > content_limit {
                            return Err(tool_error(
                                io,
                                crate::wire::ErrorCode::Conflict,
                                "durable tool invocation exceeds admitted descriptor limit",
                            ));
                        }
                        let pinned = self.load::<ToolInvocation>(reference, io).await?;
                        if pinned.0 != *invocation {
                            return Err(tool_error(
                                io,
                                crate::wire::ErrorCode::Conflict,
                                "durable tool call identity changed",
                            ));
                        }
                        dispatched = true;
                    }
                    ExecutionEvent::ToolCompleted {
                        step: 0,
                        call_id,
                        result,
                        projection,
                    } if dispatched
                        && completed.is_none()
                        && failed.is_none()
                        && *call_id == invocation.call_id =>
                    {
                        completed = Some((result, projection))
                    }
                    ExecutionEvent::ToolFailed {
                        step: 0,
                        call_id,
                        reason,
                    } if dispatched
                        && completed.is_none()
                        && failed.is_none()
                        && *call_id == invocation.call_id =>
                    {
                        failed = Some(*reason)
                    }
                    _ => {
                        return Err(tool_error(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "durable tool journal has an incompatible history",
                        ));
                    }
                }
            }
            if let Some((result, projection)) = completed {
                if result.descriptor().byte_length() > content_limit
                    || projection.descriptor().byte_length() > projection_limit
                {
                    return Err(tool_error(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "durable tool history exceeds admitted descriptor limits",
                    ));
                }
                let result = self.load::<ToolResult>(result, io).await?;
                let projection = self.load::<Value>(projection, io).await?;
                if let Err(error) = tool_json_bound(&result.0, content_limit, io)
                    .and_then(|()| tool_json_bound(&projection.0, projection_limit, io))
                {
                    if tool_propagate(&error, io) {
                        return Err(error);
                    }
                    return Err(Error::Conflict(
                        "durable tool history exceeds admitted limits".into(),
                    ));
                }
                crate::contract::validate_json_schema_value(
                    &definition.output_schema,
                    &result.0.value,
                    "tool output",
                    io,
                )?;
                tool_validate_projection(definition, &projection.0, projection_limits, io)?;
                return tool_success(result, io);
            }
            if let Some(reason) = failed {
                return tool_outcome(
                    Outcome::Failed {
                        message: String::new(),
                    },
                    Some(reason),
                    io,
                );
            }
            if reconcile_only && !dispatched {
                return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
            }
            drop(records);
            if !started {
                let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
                let event = ExecutionEvent::Started {
                    request_digest: digest,
                };
                self.append(operation_id, "tool:started", &event, io)
                    .await?;
            }
            let mut claimed = false;
            if !dispatched {
                let reference = self
                    .stage(operation_id, "tool:invocation", invocation, io)
                    .await?;
                let current = self.replay(operation_id, io).await?;
                if matches!(current.0.as_slice(), [record]
                    if matches!(&record.event, ExecutionEvent::Started { request_digest }
                        if *request_digest == digest))
                {
                    let key = tool_label("tool:claim", io)?;
                    let call = tool_string(&invocation.call_id, io)?;
                    let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
                    let event = ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: call.0,
                        invocation: reference.0,
                    };
                    match self.append_tail(operation_id, 1, &key.0, &event, io).await {
                        Ok(true) => claimed = true,
                        Ok(false) => continue,
                        Err(error) if tool_propagate(&error, io) => return Err(error),
                        Err(Error::Indeterminate(_)) => {
                            return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                        }
                        Err(error) => return Err(error),
                    }
                } else if !matches!(current.0.get(1).map(|record| &record.event),
                    Some(ExecutionEvent::ToolStarted { step: 0, call_id, .. })
                        if *call_id == invocation.call_id)
                {
                    return Err(tool_error(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "durable tool journal changed before dispatch",
                    ));
                } else {
                    continue;
                }
            }
            macro_rules! fail {
                ($reason:expr) => {
                    if let Some(outcome) = self.fail(operation_id, invocation, $reason, io).await? {
                        return Ok(outcome);
                    }
                    continue;
                };
            }
            let result = if claimed {
                match self.invoke(tool, invocation, context, io).await {
                    Ok(result) => result,
                    Err(error) if tool_propagate(&error, io) => return Err(error),
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                    }
                    Err(_) => {
                        fail!(ToolFailureKind::ExecutorRejected);
                    }
                }
            } else {
                match self.reconcile(tool, invocation, context, io).await {
                    Ok(Some(result)) => result,
                    Ok(None) => {
                        return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                    }
                    Err(error) if tool_propagate(&error, io) => return Err(error),
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                    }
                    Err(error) => return Err(error),
                }
            };
            if let Err(error) = crate::contract::validate_json_schema_value(
                &definition.output_schema,
                &result.0.value,
                "tool output",
                io,
            ) {
                if tool_propagate(&error, io) {
                    return Err(error);
                }
                fail!(ToolFailureKind::InvalidOutput);
            }
            if let Err(error) = tool_json_bound(&result.0, content_limit, io) {
                if tool_propagate(&error, io) {
                    return Err(error);
                }
                fail!(ToolFailureKind::PublicationRejected);
            }
            let projection = match tool_project(tool, invocation, &result.0, io) {
                Ok(projection) => projection,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(_) => {
                    fail!(ToolFailureKind::ProjectionRejected);
                }
            };
            if let Err(error) =
                tool_validate_projection(definition, &projection.0, projection_limits, io)
                    .and_then(|()| tool_json_bound(&projection.0, projection_limit, io))
            {
                if tool_propagate(&error, io) {
                    return Err(error);
                }
                fail!(ToolFailureKind::ProjectionRejected);
            }
            let result_ref = match self.stage(operation_id, "tool:result", &result.0, io).await {
                Ok(reference) => reference,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                }
                Err(_) => {
                    fail!(ToolFailureKind::PublicationRejected);
                }
            };
            let projection_ref = match self
                .stage(operation_id, "tool:projection", &projection.0, io)
                .await
            {
                Ok(reference) => reference,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                }
                Err(_) => {
                    fail!(ToolFailureKind::PublicationRejected);
                }
            };
            let key = tool_label("tool:completed", io)?;
            let call = tool_string(&invocation.call_id, io)?;
            let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
            let event = ExecutionEvent::ToolCompleted {
                step: 0,
                call_id: call.0,
                result: result_ref.0,
                projection: projection_ref.0,
            };
            match self.append_tail(operation_id, 2, &key.0, &event, io).await {
                Ok(true) => return tool_success(result, io),
                Ok(false) => continue,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn authorize(
        &self,
        tool: &crate::tool::Tool,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let scope = context.effective_scope();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let grant_hold;
            let (grant, hold) = work.display(&format_args!("tool:call:{}", definition.name))?;
            grant_hold = hold;
            if !scope.grants().contains(&grant) {
                return Err(tool_error(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "scope lacks tool call grant",
                ));
            }
            crate::contract::validate_json_schema_value(
                &definition.input_schema,
                &invocation.arguments,
                "tool input",
                io,
            )?;
            tool.executor
                .authorize_with_native_context(context, invocation, work)?;
            drop(grant);
            drop(grant_hold);
            return match context.authorize_tool(definition, invocation, work).await? {
                Some(operation) => Err(Error::Indeterminate(operation)),
                None => Ok(()),
            };
        }
        let required = capability::tool_call(&definition.name);
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        validate_value(
            &definition.input_schema,
            &invocation.arguments,
            "tool input",
            None,
        )?;
        tool.executor.authorize_with_context(context, invocation)?;
        match context
            .task()
            .authorize_tool(definition, invocation)
            .await?
        {
            Some(operation) => Err(Error::Indeterminate(operation)),
            None => Ok(()),
        }
    }

    async fn replay(
        &self,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Vec<crate::executor::ExecutionRecord>, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (_, records, custody) = crate::executor::replay_execution_with_native_work(
                self.journal.as_ref(),
                operation_id,
                4,
                |_| true,
                work,
            )?
            .await?;
            return Ok((records, Some(custody)));
        }
        let (_, records) =
            replay_execution(self.journal.as_ref(), operation_id, 4, |_| true, None).await?;
        Ok((records, None))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn load<
        T: crate::executor::PreparationRecord + crate::filesystem::UploadRecord + Send,
    >(
        &self,
        reference: &crate::conversation::FileRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(T, crate::NativeCustody)> {
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (value, custody) = crate::executor::load_json_with_native_work(
                self.journal.as_ref(),
                reference,
                work,
            )?
            .await?;
            return Ok((value, Some(custody)));
        }
        Ok((
            load_json::<T>(self.journal.as_ref(), reference, None)
                .await?
                .0,
            None,
        ))
    }

    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    async fn load<T: crate::executor::PreparationRecord + Send>(
        &self,
        reference: &crate::conversation::FileRef,
        _io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(T, crate::NativeCustody)> {
        Ok((
            load_json::<T>(self.journal.as_ref(), reference, None)
                .await?
                .0,
            None,
        ))
    }

    async fn stage<T: serde::Serialize + Sync>(
        &self,
        operation_id: OperationId,
        key: &str,
        value: &T,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(crate::conversation::FileRef, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (reference, custody) = crate::executor::stage_json_with_native_work(
                self.journal.as_ref(),
                operation_id,
                key,
                value,
                work,
            )?
            .await?;
            return Ok((reference, Some(custody)));
        }
        Ok((
            stage_json(self.journal.as_ref(), operation_id, key, value, None).await?,
            None,
        ))
    }

    async fn append(
        &self,
        operation_id: OperationId,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            return self
                .journal
                .append_with_native_work(operation_id, key, event, work)?
                .await;
        }
        self.journal
            .append(operation_id, key.into(), event.clone())
            .await
    }

    async fn append_tail(
        &self,
        operation_id: OperationId,
        tail: u64,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            return self
                .journal
                .append_if_tail_with_native_work(operation_id, tail, key, event, work)?
                .await;
        }
        self.journal
            .append_if_tail(operation_id, tail, key.into(), event.clone())
            .await
    }

    async fn invoke(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool
                .executor
                .execute_with_native_context(context, invocation, work)?
                .await?;
            return Ok((result, Some(custody)));
        }
        Ok((
            tool.executor
                .execute_with_context(context.clone(), invocation.clone())
                .await?,
            None,
        ))
    }

    async fn reconcile(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(ToolResult, crate::NativeCustody)>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool
                .executor
                .reconcile_with_native_context(context, invocation, work)?
                .await?;
            return Ok(result.map(|result| (result, Some(custody))));
        }
        Ok(tool
            .executor
            .reconcile_with_context(context.clone(), invocation.clone())
            .await?
            .map(|result| (result, None)))
    }

    async fn fail(
        &self,
        operation_id: OperationId,
        invocation: &ToolInvocation,
        reason: ToolFailureKind,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(Outcome<Value>, crate::NativeCustody)>> {
        let key = tool_label("tool:failed", io)?;
        let call = tool_string(&invocation.call_id, io)?;
        let event = ExecutionEvent::ToolFailed {
            step: 0,
            call_id: call.0,
            reason,
        };
        match self.append_tail(operation_id, 2, &key.0, &event, io).await {
            Ok(true) => tool_outcome(
                Outcome::Failed {
                    message: String::new(),
                },
                Some(reason),
                io,
            )
            .map(Some),
            Ok(false) => Ok(None),
            Err(error) if tool_propagate(&error, io) => Err(error),
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                tool_outcome(Outcome::Indeterminate { operation_id }, None, io).map(Some)
            }
            Err(error) => Err(error),
        }
    }
}

fn tool_propagate(error: &Error, io: Option<&crate::executor::PreparationIo>) -> bool {
    if tool_native(io) {
        return true;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        error.is_native_failure()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = error;
        false
    }
}

// Tuple values precede their allocation guards, including on early error.
fn tool_metadata<T>(io: Option<&crate::executor::PreparationIo>) -> Result<crate::NativeCustody> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        return Ok(Some(work.claim(
            std::mem::size_of::<T>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?));
    }
    Ok(None)
}

fn tool_native(io: Option<&crate::executor::PreparationIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        io.and_then(crate::executor::PreparationIo::original_native_work)
            .is_some()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = io;
        false
    }
}

pub(super) fn tool_error(
    io: Option<&crate::executor::PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if tool_native(io) {
        return Error::NativeBoundary { code, reason };
    }
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        _ => Error::Conflict(reason.into()),
    }
}

fn tool_json_bound<T: serde::Serialize>(
    value: &T,
    maximum: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let encoded_hold;
        let encoded = io.encode(value, maximum, true)?;
        encoded_hold = io.guard_bytes(&encoded)?;
        drop(encoded);
        drop(encoded_hold);
        return Ok(());
    }
    crate::contract::validate_json_byte_bound(value, maximum)
}

fn tool_validate_projection(
    definition: &ToolDefinition,
    value: &Value,
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let content = definition.validate_projection(value, limits, io)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && io.original_native_work().is_some()
    {
        let bytes = content.logical_owned_bytes()?;
        drop(content);
        return io.release(bytes);
    }
    drop(content);
    Ok(())
}

fn tool_project(
    tool: &crate::tool::Tool,
    invocation: &ToolInvocation,
    result: &ToolResult,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Value, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = tool.projection.project(invocation, result, Some(work))?;
        let custody = custody.ok_or_else(|| {
            tool_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "tool projection has no native producer",
            )
        })?;
        return Ok((value, Some(custody)));
    }
    tool.projection.project(invocation, result, None)
}

fn tool_string(
    value: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(value)?;
        return Ok((value, Some(custody)));
    }
    Ok((value.into(), None))
}

fn tool_label(
    prefix: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(String, crate::NativeCustody)> {
    let id = OperationId::new();
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(&format_args!("{prefix}:{id}"))?;
        return Ok((value, Some(custody)));
    }
    Ok((format!("{prefix}:{id}"), None))
}

fn tool_success(
    mut result: (ToolResult, crate::NativeCustody),
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Outcome<Value>, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let guard = result.1.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native tool result has no custody",
        })?;
        if !guard.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool result belongs to another work",
            });
        }
        let metadata =
            (std::mem::size_of::<Outcome<Value>>() - std::mem::size_of::<ToolResult>()) as u64;
        guard.resize_retained(guard.bytes().checked_add(metadata).ok_or(
            Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native tool outcome ownership overflow",
            },
        )?)?;
        return Ok((Outcome::Succeeded(result.0.value), result.1));
    }
    Ok((Outcome::Succeeded(result.0.value), None))
}

fn tool_outcome(
    mut outcome: Outcome<Value>,
    failure: Option<ToolFailureKind>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Outcome<Value>, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let mut custody = work.claim(
            std::mem::size_of::<Outcome<Value>>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        if let Some(reason) = failure {
            let (message, mut hold) = work.display(reason.message())?;
            custody.absorb(&mut hold)?;
            outcome = Outcome::Failed { message };
        }
        return Ok((outcome, Some(custody)));
    }
    if let Some(reason) = failure {
        outcome = Outcome::Failed {
            message: reason.message().into(),
        };
    }
    Ok((outcome, None))
}
