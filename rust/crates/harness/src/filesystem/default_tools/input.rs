//! Immutable selections over the existing operation-bound content publisher.
use super::*;
use crate::wire::ErrorCode::Unauthorized;
use crate::{
    executor::{stock_string_copy, with_history_io},
    filesystem::PreparationIo,
};

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn pinned_plan(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        owner: &RecursiveOwner<P, A, O>,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<Plan> {
        let scope = context.effective_scope();
        let digest = crate::executor::preparation_digest(
            &(
                context.durable_task_id(),
                &self.definition,
                invocation,
                scope.grants(),
                scope.limits(),
                scope.run_limits(),
                scope.extensions(),
            ),
            io,
        )?;
        let maximum = self
            .maximum_request_bytes
            .min(context.effective_scope().limits().file_bytes);
        {
            let ordinary;
            let observer = if let Some(io) = io {
                io
            } else {
                ordinary = PreparationIo::from_budget(
                    acyclic_fs::performance::work_budget(
                        self.maximum_request_bytes,
                        self.maximum_request_bytes,
                    ),
                    cancellation,
                )?;
                &ordinary
            };
            let bytes = observer.encode(&digest, maximum, true)?;
            let path = with_history_io(Some(observer), |work| {
                crate::executor::preparation_format(
                    format_args!("recursive-tools/{}/binding-v5.json", context.operation_id()),
                    work,
                )
            })?;
            let _path = observer.guard_owned(path.capacity() as u64)?;
            context
                .stage_file_once(
                    derived(invocation.operation_id, "binding-v5", io)?,
                    &path,
                    &bytes,
                    "application/json",
                    "binding-v5.json",
                    Some(observer),
                )
                .await?;
        }
        let ordinary;
        let observer = if let Some(io) = io {
            io
        } else {
            ordinary = PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(
                    self.maximum_request_bytes,
                    self.maximum_request_bytes,
                ),
                cancellation,
            )?;
            &ordinary
        };
        let operation = derived(invocation.operation_id, "plan-v5", io)?;
        let (original, _) = context.observe_staged_file_once(operation, io).await?;
        let plan = if let Some(file) = original {
            reject_if(
                file.descriptor().byte_length() > maximum,
                io,
                crate::wire::ErrorCode::Invalid,
                "retained recursive payload exceeds its bound",
            )?;
            let bytes = context.read_file(&file, Some(observer)).await?;
            crate::filesystem::retain_decoded(observer.decode(
                std::marker::PhantomData::<Plan>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?)?
        } else {
            let intent = if self.definition.name == "acyclic.fork" {
                Some(
                    self.pinned_source(context, invocation, cancellation, io)
                        .await?,
                )
            } else {
                None
            };
            let plan = self
                .plan(
                    context,
                    owner,
                    &invocation.arguments,
                    intent.as_ref(),
                    cancellation,
                    io,
                )
                .await?;
            let bytes = observer.encode(&plan, maximum, true)?;
            let path = with_history_io(Some(observer), |work| {
                crate::executor::preparation_format(
                    format_args!("recursive-tools/{}/plan-v5.json", context.operation_id()),
                    work,
                )
            })?;
            let _path = observer.guard_owned(path.capacity() as u64)?;
            context
                .stage_file_once(
                    operation,
                    &path,
                    &bytes,
                    "application/json",
                    "plan-v5.json",
                    Some(observer),
                )
                .await?;
            plan
        };
        validate_plan(&plan, io)?;
        require_plan_arguments(&plan, &invocation.arguments, io)?;
        let (caller, effect) = match &plan {
            Plan::Fork(plan) => (
                plan.caller,
                recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |request| {
                    Ok(request.operation_id)
                })?,
            ),
            Plan::Merge(plan) => (plan.caller, plan.operation_id),
        };
        deny(
            Some(caller) != context.durable_task_id() || effect != invocation.operation_id,
            io,
        )?;
        Ok(plan)
    }

    async fn pinned_source(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<RecursiveForkSourceIntent> {
        let operation = super::super::recursive::derive_operation(
            b"harness/v2/configured-recursive-checkpoint\0",
            &[invocation.operation_id.into_bytes()],
            io,
        )?;
        let ordinary;
        let observer = if let Some(io) = io {
            io
        } else {
            ordinary = PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(
                    self.maximum_request_bytes,
                    self.maximum_request_bytes,
                ),
                cancellation,
            )?;
            &ordinary
        };
        let maximum = self
            .maximum_request_bytes
            .min(context.effective_scope().limits().file_bytes);
        let (original, _) = context.observe_staged_file_once(operation, io).await?;
        let intent = if let Some(file) = original {
            reject_if(
                file.descriptor().byte_length() > maximum,
                io,
                crate::wire::ErrorCode::Invalid,
                "retained recursive payload exceeds its bound",
            )?;
            let bytes = context.read_file(&file, Some(observer)).await?;
            crate::filesystem::retain_decoded(observer.decode(
                std::marker::PhantomData::<RecursiveForkSourceIntent>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?)?
        } else {
            let arguments: ForkToolArguments = decode(&invocation.arguments, io)?;
            let intent = self
                .source
                .as_ref()
                .ok_or_else(|| {
                    recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "authorized source",
                    )
                })?
                .select(context, &arguments, cancellation, io)
                .await?;
            let bytes = observer.encode(
                &intent,
                self.maximum_request_bytes
                    .min(context.effective_scope().limits().file_bytes),
                false,
            )?;
            let path = with_history_io(Some(observer), |work| {
                crate::executor::preparation_format(
                    format_args!(
                        "recursive-tools/{}/source-checkpoint.json",
                        context.operation_id()
                    ),
                    work,
                )
            })?;
            let _path = observer.guard_owned(path.capacity() as u64)?;
            context
                .stage_file_once(
                    operation,
                    &path,
                    &bytes,
                    "application/json",
                    "source-checkpoint.json",
                    Some(observer),
                )
                .await?;
            intent
        };
        deny(
            intent.request.child_agent != intent.arguments.id
                || intent.source_task != intent.arguments.source.unwrap_or(intent.caller)
                || intent.child_operation_id == intent.request.operation_id
                || task_id_for_admission(intent.child_operation_id) == intent.caller
                || !recursive_encoded_equal(&intent.arguments, &invocation.arguments, io)?
                || Some(intent.caller) != context.durable_task_id()
                || intent.request.operation_id != invocation.operation_id,
            io,
        )?;
        Ok(intent)
    }

    pub(super) async fn plan(
        &self,
        context: &ToolContext,
        owner: &RecursiveOwner<P, A, O>,
        arguments: &Value,
        intent: Option<&RecursiveForkSourceIntent>,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<Plan> {
        let caller = context
            .durable_task_id()
            .ok_or_else(|| recursive_reject(io, Unauthorized, "authorized caller"))?;
        let (runtime, journal_owner) = owner;
        let source = self.source.as_ref().ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "authorized source",
            )
        })?;
        let plan = if self.definition.name == "acyclic.fork" {
            let original: ForkToolArguments = decode(arguments, io)?;
            with_history_io(io, |work| {
                original.source_task(caller, context.effective_scope().limits(), work)
            })?;
            let intent = intent
                .ok_or_else(|| recursive_reject(io, Unauthorized, "fork source is not pinned"))?;
            let binding = source
                .fork(context, &original, intent, None, cancellation, io)
                .await?;
            deny(
                binding.source.request != intent.request
                    || binding.child.operation_id != intent.child_operation_id,
                io,
            )?;
            validate_fork_binding(context, &original, &binding, cancellation, io).await?;
            let operation = derived(context.operation_id(), "preparation-task", io)?;
            let payload = {
                let ordinary;
                let observer = if let Some(io) = io {
                    io
                } else {
                    ordinary = PreparationIo::from_budget(
                        acyclic_fs::performance::work_budget(
                            self.maximum_request_bytes,
                            self.maximum_request_bytes,
                        ),
                        cancellation,
                    )?;
                    &ordinary
                };
                let maximum = self
                    .maximum_request_bytes
                    .min(context.effective_scope().limits().file_bytes);
                let bytes = observer.encode(&binding.source.request, maximum, true)?;
                let path = with_history_io(Some(observer), |work| {
                    crate::executor::preparation_format(
                        format_args!(
                            "recursive-tools/{}/preparation-payload.json",
                            context.operation_id()
                        ),
                        work,
                    )
                })?;
                let _path = observer.guard_owned(path.capacity() as u64)?;
                context
                    .stage_file_once(
                        derived(context.operation_id(), "preparation-payload", io)?,
                        &path,
                        &bytes,
                        "application/json",
                        "preparation-payload.json",
                        Some(observer),
                    )
                    .await?
                    .0
            };
            let request = fork_preparation_request(
                binding.source.preparer.as_ref(),
                &binding.source.request,
                task_id_for_admission(operation),
                binding.child,
                WorkflowCommand {
                    operation_id: context.operation_id(),
                    kind: stock_string_copy(FORK_PREPARE_TASK_COMMAND_KIND, io)?,
                    payload,
                },
                binding.limits,
                Some(binding.source_checkpoint),
                binding.input_files,
                io,
            )?;
            let preparation = 'admission: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                    let input_io = PreparationIo::from_native_work(work)?;
                    let input = encode(&request, Some(&input_io))?;
                    let (admission, retained) = runtime.harness().registered_admission(
                        operation,
                        "acyclic.fork_preparation",
                        "1",
                        input,
                        Some((caller, context.scope())),
                        None,
                        Some(&input_io),
                    )?;
                    io.ok_or_else(|| {
                        crate::executor::stock_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "native admission",
                        )
                    })?
                    .absorb_native_custody(input_io.take_native_custody(retained)?)?;
                    break 'admission admission;
                }
                break 'admission runtime
                    .harness()
                    .registered_admission(
                        operation,
                        "acyclic.fork_preparation",
                        "1",
                        encode(&request, io)?,
                        Some((caller, context.scope())),
                        None,
                        None,
                    )?
                    .0;
            };
            Plan::Fork(recursive_box(
                RecursiveForkToolPlan {
                    arguments: original,
                    caller,
                    request,
                    preparation,
                    child_message_pointer: binding.child_message_pointer,
                    child_message_file_pointer: binding.child_message_file_pointer,
                    allocation_digest: intent.allocation_digest,
                },
                io,
            )?)
        } else {
            let original: MergeToolArguments = decode(arguments, io)?;
            let binding = source
                .merge(context, &original, None, cancellation, io)
                .await?;
            validate_merge_binding(context, &binding, io)?;
            let controller = ParentProjectController::new(
                binding.filesystem.as_ref(),
                binding.destination.reducer(),
                &binding.verifier,
                &binding.scope,
                &binding.project,
                io,
            )?;
            let inspected = match &binding.source_read {
                Some(handle) => {
                    controller
                        .prepare_project_import_from_selected_source(
                            &binding.source_project,
                            &binding.source_generation,
                            None,
                            handle,
                            journal_owner,
                            context.task(),
                            context.operation_id(),
                        )
                        .await?
                }
                None => {
                    controller
                        .prepare_project_import(
                            &binding.source_project,
                            &binding.source_generation,
                            None,
                        )
                        .await?
                }
            };
            let expected_target_generation = crate::filesystem::fork_preparer_io::generation_ref(
                binding.project.provider(),
                inspected.target_head(),
                io,
            )?;
            let destination = match io {
                Some(io) => io
                    .history_io()
                    .clone_authority(binding.destination.reducer().authority())?,
                None => binding.destination.reducer().authority().clone(),
            };
            let authorized_source = binding
                .source_read
                .as_ref()
                .map(|source| {
                    with_history_io(io, |work| source.authorization().copy(work))
                        .map(|copied| copied.0)
                })
                .transpose()?;
            drop(controller);
            Plan::Merge(recursive_box(
                RecursiveMergeToolPlan {
                    arguments: original,
                    caller,
                    operation_id: context.operation_id(),
                    destination,
                    source: binding.source,
                    source_project: binding.source_project,
                    source_generation: binding.source_generation,
                    authorized_source,
                    target_project: binding.project,
                    expected_target_generation,
                    notice: binding.notice,
                },
                io,
            )?)
        };
        validate_plan(&plan, io)?;
        require_plan_arguments(&plan, arguments, io)?;
        if !io.is_some_and(|io| io.original_native_work().is_some()) {
            crate::executor::json::json_byte_length(
                &plan,
                self.maximum_request_bytes
                    .min(context.scope().limits().file_bytes),
                &mut None,
            )?;
        }
        Ok(plan)
    }
}
