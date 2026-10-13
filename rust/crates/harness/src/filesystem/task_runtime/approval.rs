//! Approval for the filesystem task runtime.

use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn tool_approval_ready(
        &self,
        context: &TaskContext,
        definition: &ToolDefinition,
        invocation: &crate::tool::ToolInvocation,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        let local_io = io
            .map(|io| {
                crate::filesystem::PreparationIo::from_native_work(
                    io.original_native_work().ok_or_else(|| {
                        observed::boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "approval requires original native work",
                        )
                    })?,
                )
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = local_io.as_ref();
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
        let identity = match io {
            Some(io) => {
                let work = io.original_native_work().expect("native approval IO");
                policy.supports_native_policy(work)?;
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
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(crate::filesystem::PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
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
        let id = crate::durable_host::task_interaction_id(task, operation);
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
            .map(crate::filesystem::PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let read = host.read(id, io).await?;
        let read_hold = io
            .map(|io| {
                io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    observed::boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "approval inspection custody underflows",
                    )
                })?)
            })
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
    pub(super) async fn model_approval_ready(
        &self,
        context: &TaskContext,
        turn: OperationId,
        input: &crate::filesystem::ModelTaskCommand,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        use crate::executor::{ExecutionEvent, ExecutionReplay, ModelPurpose};
        #[cfg(not(target_arch = "wasm32"))]
        let local_io = io
            .map(|io| {
                crate::filesystem::PreparationIo::from_native_work(
                    io.original_native_work().ok_or_else(|| {
                        observed::boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "model approval requires original native work",
                        )
                    })?,
                )
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = local_io.as_ref();
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
        let (journal, operation) = match io {
            #[cfg(not(target_arch = "wasm32"))]
            Some(io) => {
                self.readonly_execution_journal_with_native_work(
                    task,
                    turn,
                    io.original_native_work().expect("native approval IO"),
                )
                .await?
            }
            _ => {
                let operation = execution_operation(task, turn, None)?;
                let journal = FilesystemExecutionJournal::new(
                    self.stream.clone(),
                    Arc::clone(&self.filesystem),
                    &self.volume,
                    &self.verifier,
                    &crate::core::SchemaRegistry::new(),
                    &self.signed,
                    self.maximum_payload_bytes.min(limits.file_bytes),
                    None,
                )?
                .with_turn(operation, turn);
                (Arc::new(journal), operation)
            }
        };
        let mut replay = ExecutionReplay::new(operation);
        let mut latest = None;
        while let Some(page) = replay.next_page(journal.as_ref(), io).await? {
            let mut history = io.map(crate::filesystem::PreparationIo::history_io);
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
        let records = match io {
            #[cfg(not(target_arch = "wasm32"))]
            Some(io) => {
                let (_, records, custody) = crate::executor::replay_execution_with_native_work(
                    journal.as_ref(),
                    operation,
                    limits.model_events_per_step.saturating_add(1),
                    select,
                    io.original_native_work().expect("native approval IO"),
                )?
                .await?;
                io.absorb_native_custody(custody)?;
                records
            }
            _ => {
                crate::executor::replay_execution(
                    journal.as_ref(),
                    operation,
                    limits.model_events_per_step.saturating_add(1),
                    select,
                    io,
                )
                .await?
                .1
            }
        };
        let retained = match io {
            #[cfg(not(target_arch = "wasm32"))]
            Some(io) => {
                let (retained, custody) = crate::executor::retained_model_step_with_native_work(
                    journal.as_ref(),
                    &records,
                    step,
                    ModelPurpose::Response,
                    limits,
                    io.original_native_work().expect("native approval IO"),
                )?
                .await?;
                io.absorb_native_custody(custody)?;
                retained
            }
            _ => {
                crate::executor::retained_model_step(
                    journal.as_ref(),
                    &records,
                    step,
                    ModelPurpose::Response,
                    limits,
                    io,
                )
                .await?
            }
        };
        drop(records);
        if !retained.admission.completed {
            return Err(Error::Indeterminate(operation));
        }
        let request = retained.request.ok_or(Error::Indeterminate(operation))?;
        for message in &request.request().messages {
            match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) => {
                    verify_native_model_files(
                        context,
                        &message.content,
                        io.original_native_work().expect("native approval IO"),
                    )
                    .await?
                }
                _ => {
                    for file in message.content.file_refs() {
                        context.read_file(file).await?;
                    }
                }
            }
        }
        for event in retained.events {
            if let crate::model::ModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            } = event
            {
                let invocation = match io {
                    #[cfg(not(target_arch = "wasm32"))]
                    Some(io) => crate::tool::ToolInvocation::for_model_call(
                        operation,
                        step,
                        call_id,
                        name,
                        arguments,
                        Some(io.original_native_work().expect("native approval IO")),
                    )?,
                    _ => crate::tool::ToolInvocation::for_model_call(
                        operation, step, call_id, name, arguments, None,
                    )?,
                };
                let select = |event: &ExecutionEvent| {
                    matches!(event,
                    ExecutionEvent::ToolStarted { step: recorded, call_id, .. }
                    | ExecutionEvent::ToolCompleted { step: recorded, call_id, .. }
                    | ExecutionEvent::ToolFailed { step: recorded, call_id, .. }
                    if *recorded == step && call_id == &invocation.call_id)
                };
                let records = match io {
                    #[cfg(not(target_arch = "wasm32"))]
                    Some(io) => {
                        let (_, records, custody) =
                            crate::executor::replay_execution_with_native_work(
                                journal.as_ref(),
                                operation,
                                3,
                                select,
                                io.original_native_work().expect("native approval IO"),
                            )?
                            .await?;
                        io.absorb_native_custody(custody)?;
                        records
                    }
                    _ => {
                        crate::executor::replay_execution(
                            journal.as_ref(),
                            operation,
                            3,
                            select,
                            io,
                        )
                        .await?
                        .1
                    }
                };
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
                    match io {
                        #[cfg(not(target_arch = "wasm32"))]
                        Some(io) => {
                            let projection =
                                crate::executor::completed_tool_projection_with_native_work(
                                    journal.as_ref(),
                                    &records,
                                    step,
                                    &invocation,
                                    &request.request().tools,
                                    limits,
                                    io.original_native_work().expect("native approval IO"),
                                )?
                                .await?;
                            drop(projection);
                        }
                        _ => {
                            crate::executor::completed_tool_projection(
                                journal.as_ref(),
                                &records,
                                step,
                                &invocation,
                                &request.request().tools,
                                crate::conversation::Limits::default(),
                                io,
                            )
                            .await?;
                        }
                    }
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
                    crate::contract::preparation_format(
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
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn verify_native_model_files(
    context: &TaskContext,
    content: &crate::model::ModelContent,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    use crate::model::{ModelContentPart, ModelDataPart, ToolResultContent};
    for part in content.parts() {
        match part {
            ModelContentPart::File { file, policy } => {
                verify_native_model_file(context, file, policy, work).await?
            }
            ModelContentPart::ToolResult {
                content: ToolResultContent::Parts { parts },
                ..
            } => {
                for part in parts {
                    if let ModelDataPart::File { file, policy } = part {
                        verify_native_model_file(context, file, policy, work).await?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn verify_native_model_file(
    context: &TaskContext,
    file: &crate::conversation::FileRef,
    policy: &crate::model::FileProjectionPolicy,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let body = context.read_file_with_native_work(file, work).await?;
    drop(body);
    if let crate::model::FileProjectionPolicy::Native(policy) = policy
        && let Some(binding) = &policy.configuration
    {
        let body = context
            .read_file_with_native_work(&binding.configuration.content, work)
            .await?;
        drop(body);
    }
    Ok(())
}
