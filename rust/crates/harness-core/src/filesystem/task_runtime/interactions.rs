use super::*;

// A submission consumes the same authenticated private view that selected its
// review. It cannot re-enter the public writer and reconstruct that view again.
struct ApprovalBinding<P> {
    task: TaskId,
    admission: TaskAdmissionRecord,
    files: Arc<crate::task_volume::TaskFiles<P>>,
}

#[derive(Clone, Copy)]
enum ApprovalWait {
    Absent,
    Pending,
    Resolved,
}

#[cfg(test)]
std::thread_local! {
    pub(super) static APPROVAL_BINDINGS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Policy pinned by this runtime and every new task admission.
    pub fn approval_policy_identity(&self) -> Option<crate::registry::ComponentIdentity> {
        self.policy.as_ref().map(|policy| policy.identity())
    }

    /// The admitted task's private files, including authenticated child resources.
    /// This passive view never acquires the executor's lease.
    pub fn approval_files(
        &self,
        task: TaskId,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<Arc<crate::task_volume::TaskFiles<P>>>> {
        Box::pin(async move { Ok(self.approval_binding(task).await?.files) })
    }

    async fn approval_binding(&self, task: TaskId) -> Result<ApprovalBinding<P>> {
        let files = self
            .task_files
            .as_ref()
            .ok_or_else(|| Error::Unsupported("approval files are not bound".into()))?;
        let admission = self.admission_observed(task, None).await?;
        #[cfg(test)]
        APPROVAL_BINDINGS.with(|count| count.set(count.get() + 1));
        let (scope, _) = self.host.authenticated_scope(&admission, &mut None).await?;
        let files = match files.for_authenticated_scope(task, &scope, &scope).await? {
            Some(files) => Arc::new(files),
            None => files.clone(),
        };
        Ok(ApprovalBinding {
            task,
            admission,
            files,
        })
    }

    /// Submits an authenticated decision without borrowing the executor's lease.
    /// The request must already exist in this task's owner-published file view.
    pub fn write_approval_decision<'a>(
        &'a self,
        task: TaskId,
        file: &'a crate::task_volume::FileValue,
        responder: &'a Scope,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let binding = self.approval_binding(task).await?;
            self.write_bound_approval(binding, file, responder).await
        })
    }

    async fn write_bound_approval(
        &self,
        binding: ApprovalBinding<P>,
        file: &crate::task_volume::FileValue,
        responder: &Scope,
    ) -> Result<()> {
        let ApprovalBinding {
            task,
            admission,
            files,
        } = binding;
        if !admission
            .grants
            .contains(crate::contract::capability::INTERACTION_ROUTE)
            || admission.policy != self.approval_policy_identity()
            || admission.policy.is_none()
        {
            return Err(Error::Unauthorized(
                "task has no matching approval policy".into(),
            ));
        }
        if self.outcome_observed(task, None).await?.is_some() {
            let previous = files
                .snapshot(&crate::task_volume::Query::File(file.key.clone()), 1)
                .await?;
            let [previous] = previous.files.as_slice() else {
                return Err(Error::Conflict("task is already terminal".into()));
            };
            // The owner stages the decoded decision in its own JSON encoding.
            // Compare that exact value: an already admitted client's bytes may
            // differ in whitespace or key order even on an identical retry.
            if crate::interaction::ApprovalDecisionFile::decode(
                &previous.bytes,
                self.maximum_payload_bytes,
            )? != crate::interaction::ApprovalDecisionFile::decode(
                &file.bytes,
                self.maximum_payload_bytes,
            )? {
                return Err(Error::Conflict("task is already terminal".into()));
            }
        }
        let owner = self
            .interaction_owner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("task conversation owner is not bound".into()))?;
        let journal = FilesystemExecutionJournal::new(
            self.stream.clone(),
            self.filesystem.clone(),
            &SchemaRegistry::new(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: self.maximum_payload_bytes.min(admission.limits.file_bytes),
                io: None,
            },
        )?
        .for_approval_projection(task)
        .with_optional_task_files(Some(files))?
        .with_interaction_owner(
            &owner.verifier,
            &owner.schemas,
            &owner.scope,
            &owner.volume,
            None,
        )?;
        journal.write_approval_decision(file, responder).await
    }

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
            approval_authority: None,
            verifier,
            schemas,
            scope,
            volume: private_volume,
        });
        self.interaction_host(None)?;
        Ok(self)
    }

    /// Transfers the approval signer into the runtime's interaction owner.
    /// Clients retain policy data and cannot issue principals or capabilities.
    pub fn with_approval_authority(
        mut self,
        issuer: &crate::core::AuthorityRoot,
    ) -> Result<Self> {
        let owner = self
            .interaction_owner
            .as_mut()
            .ok_or_else(|| Error::Unsupported("approval conversation owner is not bound".into()))?;
        if owner.approval_authority.is_some() || issuer.verifier(&mut None)? != owner.verifier {
            return Err(Error::Unauthorized(
                "approval authority differs from its owner".into(),
            ));
        }
        owner.approval_authority = Some(issuer.copy(&mut None)?);
        Ok(self)
    }

    /// Settles a policy service's decision using its immutable, bound review.
    /// The runtime issues only this reviewed principal's exact responder grant.
    pub fn submit_reviewed_approval<'a>(
        &'a self,
        task: TaskId,
        file: &'a crate::task_volume::FileValue,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            use crate::task_volume::{Component, FileKey, Query, TaskFile};
            let FileKey::Task {
                task: target,
                file: TaskFile::ApprovalDecision { request, .. },
            } = &file.key
            else {
                return Err(Error::Unauthorized(
                    "policy service requires a decision file".into(),
                ));
            };
            let decision = crate::interaction::ApprovalDecisionFile::decode(
                &file.bytes,
                self.maximum_payload_bytes,
            )?;
            let key = FileKey::Task {
                task: target.clone(),
                file: TaskFile::ApprovalReview {
                    request: request.clone(),
                    principal: Component::new(
                        crate::interaction::ApprovalDecisionFile::principal_file(
                            &decision.principal,
                        ),
                    )?,
                },
            };
            let binding = self.approval_binding(task).await?;
            let snapshot = binding.files.snapshot(&Query::File(key), 1).await?;
            let [review] = snapshot.files.as_slice() else {
                return Err(Error::Unauthorized(
                    "policy service has no admitted review".into(),
                ));
            };
            let review: crate::approvals::ApprovalReviewFile =
                crate::contract::json_from_slice(&review.bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            let policy = self
                .approval_policy_identity()
                .ok_or_else(|| Error::Unauthorized("approval policy is absent".into()))?;
            if target.as_str() != task.to_string()
                || review.principal != decision.principal
                || review.request_sha256 != decision.request_sha256
                || review.policy_digest != policy.digest
                || match review.verdict {
                    crate::approvals::RuleVerdict::Allow => !decision.approved(),
                    crate::approvals::RuleVerdict::Deny => decision.approved(),
                    crate::approvals::RuleVerdict::Ask => false,
                }
            {
                return Err(Error::Unauthorized(
                    "decision differs from its admitted review".into(),
                ));
            }
            let issuer = self
                .interaction_owner
                .as_ref()
                .and_then(|owner| owner.approval_authority.as_ref())
                .ok_or_else(|| {
                    Error::Unsupported("embedding must supply a signed approval decision".into())
                })?;
            let id = crate::InteractionId::from_bytes(
                uuid::Uuid::parse_str(request.as_str())
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into_bytes(),
            );
            let responder = issuer.bootstrap_scope(
                &review.principal,
                &crate::Capabilities::new([
                    "interaction:resolve".to_owned(),
                    format!("interaction:respond:{id}"),
                ]),
                None,
                &mut None,
            )?;
            self.write_bound_approval(binding, file, &responder).await
        })
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
        Ok(matches!(
            self.tool_approval_status(context, definition, invocation, io)
                .await?,
            ApprovalWait::Resolved
        ))
    }

    async fn tool_approval_status(
        &self,
        context: &TaskContext,
        definition: &ToolDefinition,
        invocation: &crate::tool::ToolInvocation,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<ApprovalWait> {
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
            return Ok(ApprovalWait::Absent);
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
        self.approval_wait(
            context,
            &identity,
            definition,
            invocation,
            io,
            crate::interaction::ApprovalRoute::Policy,
        )
        .await
    }

    // Readiness only observes the exact owner's ticket. It never authorizes
    // execution; the stock executor consumes the durable decision again.
    async fn approval_wait(
        &self,
        context: &TaskContext,
        identity: &crate::registry::ComponentIdentity,
        definition: &ToolDefinition,
        invocation: &crate::tool::ToolInvocation,
        io: Option<&super::super::PreparationIo>,
        expected_route: crate::interaction::ApprovalRoute,
    ) -> Result<ApprovalWait> {
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("approval wait has no task".into()))?;
        let work = io.and_then(super::super::PreparationIo::original_native_work);
        let (operation, digest) =
            crate::runtime::tool_approval_binding(task, identity, definition, invocation, work)?;
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
        // Request identity and terminal status must belong to the same cut.
        // This is an observation; execution still consumes the durable decision.
        let aggregate = host.aggregate(io).await?;
        match host.read_request_at(&aggregate.0, id, io).await? {
            None => return Ok(ApprovalWait::Absent),
            Some(Interaction::Approval {
                operation_id,
                action_digest,
                subject:
                    crate::interaction::ApprovalSubject::Tool {
                        route,
                        tool,
                        call_id,
                        arguments,
                    },
                ..
            }) if route == expected_route
                && operation_id == invocation.operation_id
                && action_digest == digest
                && tool == invocation.name
                && call_id == invocation.call_id
                && arguments == invocation.arguments => {}
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
        let read = host.read_outcome(&aggregate.0, id, io)?;
        let read_hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        let ready = read
            .as_ref()
            .is_some_and(crate::interaction::InteractionOutcome::is_terminal);
        drop(read);
        drop(read_hold);
        Ok(if ready {
            ApprovalWait::Resolved
        } else {
            ApprovalWait::Pending
        })
    }

    /// Inspects retained artifacts without an executor or passive task lease.
    pub(in crate::filesystem) async fn model_approval_ready(
        &self,
        context: &TaskContext,
        turn: OperationId,
        input: &super::super::ModelTaskCommand,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<bool> {
        use crate::executor::{ExecutionEvent, ExecutionRecordQuery, ModelPurpose};
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
        let Some(step) = journal.latest_response_step(operation, io).await? else {
            return Ok(false);
        };
        if step >= input.max_steps {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "retained model step exceeds command",
            ));
        }
        let select = |event: &ExecutionEvent| {
            matches!(event,
            ExecutionEvent::ModelStarted { step: recorded, purpose: ModelPurpose::Response, .. }
            | ExecutionEvent::Model { step: recorded, purpose: ModelPurpose::Response, .. }
            | ExecutionEvent::ModelInline { step: recorded, purpose: ModelPurpose::Response, .. } if *recorded == step)
        };
        let (_, records, hold) = crate::executor::selected_step_records(
            journal.as_ref(),
            ExecutionRecordQuery {
                operation,
                step,
                include_started: false,
                maximum_step_records: ExecutionRecordQuery::step_bound(limits),
                maximum: limits.model_events_per_step.saturating_add(1),
                select: &select,
            },
            io,
        )
        .await?;
        let records = crate::executor::retain_preparation((records, hold))?;
        // The ordinary passive reader has no fenced owner embedded in the
        // journal. Check the selected dispatch against its scheduler claim on
        // every read; the signed checkpoint authenticates only the old prefix.
        let mut history = io.map(super::super::PreparationIo::history_io);
        self.host
            .verify_model_history(
                task,
                operation,
                records.iter(),
                &mut history
                    .as_mut()
                    .map(|history| history as &mut dyn crate::store::history::HistoryIo),
            )
            .await?;
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
                let (_, records, hold) = crate::executor::selected_step_records(
                    journal.as_ref(),
                    ExecutionRecordQuery {
                        operation,
                        step,
                        include_started: false,
                        maximum_step_records: ExecutionRecordQuery::step_bound(limits),
                        maximum: 3,
                        select: &select,
                    },
                    io,
                )
                .await?;
                let records = crate::executor::retain_preparation((records, hold))?;
                // A failed call is settled: the model received the failure as its result.
                if records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolFailed { .. }))
                {
                    continue;
                }
                #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
                let (invocation, pre_hook) = {
                    let mut invocation = invocation;
                    let mut output = if io.is_none() {
                        self.retained_pre_tool_hook(context, &invocation).await?
                    } else {
                        None
                    };
                    if let Some(output) = &mut output
                        && !output.blocks()
                        && let Some(arguments) = output.updated_input.take()
                    {
                        invocation.arguments = arguments;
                    }
                    (invocation, output)
                };
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
                // Calls the executor refuses before admission (an undeclared or ungranted tool,
                // or arguments outside the pinned schema) settle as model-visible results and
                // never wait for approval.
                let Some(resolved) = self.tools.get(&invocation.name, None, io)? else {
                    continue;
                };
                let definition = &resolved.definition;
                let Some(pinned) = request.request().tools.iter().find(|pinned| {
                    pinned.name == definition.name && pinned.revision == definition.revision
                }) else {
                    continue;
                };
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
                if !allowed
                    || crate::executor::with_history_io(io, |work| {
                        crate::executor::check_json_schema_value(
                            &pinned.input_schema,
                            &invocation.arguments,
                            "tool input",
                            work,
                        )
                    })?
                    .is_err()
                {
                    continue;
                }
                #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
                if pre_hook
                    .as_ref()
                    .is_some_and(crate::hooks::HookOutput::blocks)
                {
                    return Ok(true);
                }
                match self
                    .tool_approval_status(context, definition, &invocation, io)
                    .await?
                {
                    ApprovalWait::Resolved => return Ok(true),
                    ApprovalWait::Pending => return Ok(false),
                    ApprovalWait::Absent => {
                        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
                        if pre_hook
                            .as_ref()
                            .is_some_and(|output| output.approval.is_some())
                        {
                            let identity = crate::hooks::effects::approval_identity(
                                *input.context_manifest.descriptor().sha256(),
                            );
                            return Ok(matches!(
                                self.approval_wait(
                                    context,
                                    &identity,
                                    definition,
                                    &invocation,
                                    io,
                                    crate::interaction::ApprovalRoute::Human
                                )
                                .await?,
                                ApprovalWait::Resolved
                            ));
                        }
                        return Ok(false);
                    }
                }
            }
        }
        Ok(false)
    }

    #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
    async fn retained_pre_tool_hook(
        &self,
        context: &TaskContext,
        invocation: &crate::tool::ToolInvocation,
    ) -> Result<Option<crate::hooks::HookOutput>> {
        if context.task_files().is_none() {
            return Ok(None);
        }
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("hook wait has no task".into()))?;
        let files = self.approval_files(task).await?;
        let event = crate::hooks::HookEvent::PreToolUse;
        // The same claim/outcome owner serves passive observation. Starting is
        // disabled, so a wait check cannot acquire or execute a hook claim.
        crate::hooks::effects::run_once::<_, _, _, _, _, true>(
            &files.inner,
            (
                task,
                crate::hooks::effects::phase_operation(invocation.operation_id, event),
                event.name(),
            ),
            context.scope().limits().file_bytes,
            false,
            || std::future::ready(Ok((Vec::new(), None))),
            || {
                std::future::ready(Err(Error::Invalid(
                    "passive hook observation attempted execution".into(),
                )))
            },
        )
        .await
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
