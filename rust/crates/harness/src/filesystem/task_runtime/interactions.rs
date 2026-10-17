use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds the existing conversation owner for task approval/question tickets.
    pub fn with_interaction_owner(
        mut self,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        scope: Scope,
        private_volume: VolumeRef,
    ) -> Result<Self> {
        verifier.verify(&scope, &mut None)?;
        if !scope
            .capabilities()
            .contains(crate::contract::capability::INTERACTION_OPEN)
        {
            return Err(Error::Unauthorized(
                "interaction owner lacks interaction:open".into(),
            ));
        }
        self.interaction_owner = Some(TaskInteractionOwner {
            verifier,
            schemas,
            scope,
            volume: private_volume,
        });
        self.interaction_host(None)?;
        Ok(self)
    }

    pub(super) fn interaction_host(
        &self,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<FilesystemInteractionHost<P, A, O>> {
        let owner = self.interaction_owner.as_ref().ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task conversation owner is not bound",
            )
        })?;
        FilesystemInteractionHost::new(
            self.stream.clone(),
            Arc::clone(&self.filesystem),
            owner.verifier.audience(),
            &owner.schemas,
            crate::filesystem::FilesystemJournalBinding {
                volume: &owner.volume,
                verifier: &owner.verifier,
                scope: &owner.scope,
                maximum_bytes: self.maximum_payload_bytes,
                io,
            },
        )
    }

    pub(in crate::filesystem) async fn tool_approval_ready(
        &self,
        context: &TaskContext,
        definition: &ToolDefinition,
        invocation: &crate::tool::ToolInvocation,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        let local_io = approval_io(io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = local_io.as_ref();
        let work = io.and_then(super::super::PreparationIo::original_native_work);
        if !context
            .scope()
            .grants()
            .contains(crate::contract::capability::INTERACTION_ROUTE)
        {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks interaction:route",
            ));
        }
        let Some(policy) = &self.policy else {
            return Ok(false);
        };
        let task = context.durable_task_id().ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "approval has no task",
            )
        })?;
        let admission = self.admission_observed(task, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let identity = match io.zip(work) {
            Some((io, work)) => {
                let (identity, custody) = policy.identity_with_native_work(work)?;
                io.absorb_native_custody(custody)?;
                identity
            }
            None => policy.identity(),
        };
        #[cfg(target_arch = "wasm32")]
        let identity = policy.identity();
        if admission.policy.as_ref() != Some(&identity) {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval policy differs from admission",
            ));
        }
        let (operation, digest) =
            crate::runtime::tool_approval_binding(task, &identity, definition, invocation, work)?;
        if let Some(io) = io {
            let hashed = acyclic_fs::WorkCounters {
                bytes_hashed: b"harness/v2/task-interaction\0".len() as u64 + 32,
                ..Default::default()
            };
            io.preflight(hashed)?;
            io.charge(hashed)?;
        }
        let id = crate::durable_host::task_interaction_id(task, operation, &mut None)?;
        let host = self.interaction_host(io)?;
        match host.read_request(id, io).await? {
            None => return Ok(false),
            Some(Interaction::Approval {
                operation_id,
                action_digest,
                ..
            }) if operation_id == invocation.operation_id && action_digest == digest => {}
            Some(_) => {
                return Err(observed::boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "approval request changed",
                ));
            }
        }
        let before = io
            .map(super::super::PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let read = host.read(id, io).await?;
        let read_hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        let ready = read
            .as_ref()
            .and_then(|(_, resolution)| resolution.as_ref())
            .is_some_and(|resolution| resolution.outcome.is_terminal());
        drop(read);
        drop(read_hold);
        Ok(ready)
    }

    /// Inspects retained artifacts without an executor or passive task lease.
    pub(in crate::filesystem) async fn model_approval_ready(
        &self,
        context: &TaskContext,
        turn: OperationId,
        input: &super::super::ModelTaskCommand,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<bool> {
        use crate::executor::{ExecutionEvent, ExecutionReplay, ModelPurpose};
        #[cfg(not(target_arch = "wasm32"))]
        let local_io = approval_io(io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = local_io.as_ref();
        let work = io.and_then(super::super::PreparationIo::original_native_work);
        let scope = context.scope();
        if !scope.grants().contains("model:generate") {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks model:generate",
            ));
        }
        let limits = scope.limits();
        if input.max_steps == 0
            || u64::from(input.max_steps) > limits.model_steps as u64
            || scope
                .run_limits()
                .max_steps
                .is_some_and(|bound| u64::from(input.max_steps) > bound as u64)
        {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model wait exceeds admitted steps",
            ));
        }
        let task = context.durable_task_id().ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model wait has no task",
            )
        })?;
        let (journal, operation) = match work {
            #[cfg(not(target_arch = "wasm32"))]
            Some(work) => {
                self.readonly_execution_journal_with_native_work(task, turn, work)
                    .await?
            }
            _ => {
                let operation = execution_operation(task, turn, None)?;
                let journal = FilesystemExecutionJournal::new(
                    self.stream.clone(),
                    Arc::clone(&self.filesystem),
                    &crate::core::SchemaRegistry::new(),
                    crate::filesystem::FilesystemJournalBinding {
                        volume: &self.volume,
                        verifier: &self.verifier,
                        scope: &self.signed,
                        maximum_bytes: self.maximum_payload_bytes.min(limits.file_bytes),
                        io: None,
                    },
                )?
                .with_turn(operation, turn);
                (Arc::new(journal), operation)
            }
        };
        let mut replay = ExecutionReplay::new(operation);
        let mut latest = None;
        while let Some(page) = replay.next_page(journal.as_ref(), io).await? {
            let mut history = io.map(super::super::PreparationIo::history_io);
            self.host
                .verify_model_history(
                    task,
                    operation,
                    page.iter(),
                    &mut history
                        .as_mut()
                        .map(|history| history as &mut dyn crate::store::history::HistoryIo),
                )
                .await?;
            for record in page {
                if let ExecutionEvent::ModelStarted {
                    step,
                    purpose: ModelPurpose::Response,
                    ..
                } = record.event
                {
                    if step >= input.max_steps {
                        return Err(observed::boundary(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "retained model step exceeds command",
                        ));
                    }
                    latest = Some(latest.map_or(step, |previous: u32| previous.max(step)));
                }
            }
        }
        let Some(step) = latest else {
            return Ok(false);
        };
        let select = |event: &ExecutionEvent| {
            matches!(event,
            ExecutionEvent::ModelStarted { step: recorded, purpose: ModelPurpose::Response, .. }
            | ExecutionEvent::Model { step: recorded, purpose: ModelPurpose::Response, .. } if *recorded == step)
        };
        let (_, records, hold) = crate::executor::replay_execution(
            journal.as_ref(),
            operation,
            limits.model_events_per_step.saturating_add(1),
            select,
            io,
        )
        .await?;
        let records = crate::executor::retain_preparation((records, hold))?;
        let retained = crate::executor::retain_preparation(
            crate::executor::retained_model_step(
                journal.as_ref(),
                &records,
                step,
                ModelPurpose::Response,
                limits,
                io,
            )
            .await?,
        )?;
        drop(records);
        if !retained.admission.completed {
            return Err(Error::Indeterminate(operation));
        }
        let request = retained.request.ok_or(Error::Indeterminate(operation))?;
        for message in &request.request().messages {
            match work {
                #[cfg(not(target_arch = "wasm32"))]
                Some(work) => {
                    verify_native_model_files(context, &message.content, work).await?;
                }
                _ => {
                    for file in message.content.file_refs() {
                        context.read_file(file, None).await?;
                    }
                }
            }
        }
        for event in retained.events {
            if let crate::model::ModelEvent::ToolCall(crate::model::ToolCall {
                call_id,
                name,
                arguments,
                metadata,
            }) = event
            {
                let invocation = crate::tool::ToolInvocation::for_model_call(
                    operation, step, call_id, name, arguments, work,
                )?;
                let select = |event: &ExecutionEvent| {
                    matches!(event,
                    ExecutionEvent::ToolStarted { step: recorded, call_id, .. }
                    | ExecutionEvent::ToolCompleted { step: recorded, call_id, .. }
                    | ExecutionEvent::ToolFailed { step: recorded, call_id, .. }
                    if *recorded == step && call_id == &invocation.call_id)
                };
                let (_, records, hold) =
                    crate::executor::replay_execution(journal.as_ref(), operation, 3, select, io)
                        .await?;
                let records = crate::executor::retain_preparation((records, hold))?;
                if records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolFailed { .. }))
                {
                    return Err(observed::boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "retained model tool failed",
                    ));
                }
                if records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
                {
                    let native = io.is_some_and(|io| io.original_native_work().is_some());
                    let projection = crate::executor::completed_tool_projection(
                        journal.as_ref(),
                        &records,
                        step,
                        crate::tool::RecordedInvocation {
                            invocation: &invocation,
                            metadata: &metadata,
                        },
                        &request.request().tools,
                        if native {
                            limits
                        } else {
                            crate::conversation::Limits::default()
                        },
                        io,
                    )
                    .await?;
                    let projection_hold = io
                        .filter(|_| native)
                        .map(|io| io.guard_owned(projection.logical_owned_bytes()?))
                        .transpose()?;
                    drop(projection);
                    drop(projection_hold);
                    continue;
                }
                if !records.is_empty() {
                    return Err(Error::Indeterminate(invocation.operation_id));
                }
                let pinned = request
                    .request()
                    .tools
                    .iter()
                    .find(|definition| definition.name == invocation.name)
                    .ok_or_else(|| {
                        observed::boundary(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "approval tool is absent from model request",
                        )
                    })?;
                let definition = self.tool_definition(&pinned.name, &pinned.revision, io)?;
                if definition != pinned {
                    return Err(observed::boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "approval tool differs from model request",
                    ));
                }
                let grant = crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(
                        format_args!("tool:call:{}", pinned.name),
                        work,
                    )
                })?;
                let grant_hold = io
                    .map(|io| io.guard_owned(grant.capacity() as u64))
                    .transpose()?;
                let allowed = scope.grants().contains(&grant);
                drop(grant);
                drop(grant_hold);
                if !allowed {
                    return Err(observed::boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "model wait lacks tool capability",
                    ));
                }
                return self
                    .tool_approval_ready(context, definition, &invocation, io)
                    .await;
            }
        }
        Ok(false)
    }

    pub(in crate::filesystem) fn bind_context_interactions(
        &self,
        mut context: TaskContext,
        task: TaskId,
        journal: Arc<FilesystemExecutionJournal<P, A, O>>,
    ) -> Result<TaskContext> {
        if self.interaction_owner.is_some() {
            if context.durable_task_id() != Some(task) {
                return Err(Error::Unauthorized(
                    "interaction owner differs from task context".into(),
                ));
            }
            context.owned_interactions = Some(journal);
            Ok(context)
        } else {
            Ok(context)
        }
    }

    /// Binds tool tickets to the original task, journal and native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_native_tool_interactions(
        &self,
        context: crate::runtime::ToolContext,
        task: TaskId,
        fence: &LeaseFence,
        turn: OperationId,
        journal: Arc<FilesystemExecutionJournal<P, A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::runtime::ToolContext> {
        if !context.native_metadata_matches(work)
            || !context.task().original_task_binding_matches(task, fence)
            || !journal.original_native_execution_work_matches(work)
            || !journal.original_task_execution_binding_matches(
                task,
                fence,
                turn,
                context.operation_id(),
                self.host.as_ref(),
            )
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "journal interaction route differs from original task",
            });
        }
        if self.interaction_owner.is_some() {
            journal.supports_native_interactions(work)?;
            context.with_native_interactions(journal, work)
        } else {
            Ok(context)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn approval_io(
    io: Option<&super::super::PreparationIo>,
) -> Result<Option<super::super::PreparationIo>> {
    io.map(|io| {
        let work = io.original_native_work().ok_or_else(|| {
            observed::boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "approval requires original native work",
            )
        })?;
        super::super::PreparationIo::from_native_work(work)
    })
    .transpose()
}
