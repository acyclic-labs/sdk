use super::*;
use crate::executor::stock_string_copy;
#[cfg(not(target_arch = "wasm32"))]
use crate::filesystem::PreparationIo;
use crate::runtime::TaskSpawner as _;

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn run_fork(
        &self,
        context: &ToolContext,
        owner: &RecursiveOwner<P, A, O>,
        fork: &RecursiveForkToolPlan,
        cancellation: &CancellationToken,
        output_io: Option<&super::super::PreparationIo>,
    ) -> Result<Option<Value>> {
        let (runtime, journal_owner) = owner;
        let temporary_io = crate::filesystem::PreparationIo::temporary(output_io)?;
        let io = temporary_io.as_ref();
        #[cfg(not(target_arch = "wasm32"))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            let original = context.native_task().ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "admission has no original task",
                )
            })?;
            deny(
                !original
                    .original_task_binding_matches(fork.caller, journal_owner.task_binding().1),
                io,
            )?;
        }
        let intent = RecursiveForkSourceIntent {
            arguments: ForkToolArguments {
                id: fork.arguments.id,
                msg: stock_string_copy(&fork.arguments.msg, io)?,
                source: fork.arguments.source,
            },
            caller: fork.caller,
            source_task: fork.arguments.source.unwrap_or(fork.caller),
            child_operation_id: fork.request.child.operation_id,
            allocation_digest: fork.allocation_digest,
            request: decode(&fork.request.payload, io)?,
        };
        let mut binding = self
            .source
            .as_deref()
            .ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "recursive source was authorized",
                )
            })?
            .fork(
                context,
                &fork.arguments,
                &intent,
                Some(fork),
                cancellation,
                io,
            )
            .await?;
        validate_fork_binding(context, &fork.arguments, &binding, cancellation, io).await?;
        deny(
            !recursive_encoded_equal(&binding.source.request, &fork.request.payload, io)?
                || binding.child_message_pointer != fork.child_message_pointer
                || binding.child_message_file_pointer != fork.child_message_file_pointer
                || fork.request.source_checkpoint.as_ref() != Some(&binding.source_checkpoint),
            io,
        )?;
        let command = fork.request.command.as_ref().ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "original preparation command missing",
            )
        })?;
        let restored = fork_preparation_request(
            binding.source.preparer.as_ref(),
            &binding.source.request,
            fork.preparation.task_id(),
            binding.child,
            WorkflowCommand {
                operation_id: command.operation_id,
                kind: stock_string_copy(&command.kind, io)?,
                payload: crate::executor::with_history_io(io, |work| command.payload.copy(work))?,
            },
            binding.limits,
            Some(binding.source_checkpoint),
            binding.input_files,
            io,
        )?;
        deny(restored != fork.request, io)?;
        let record = &fork.preparation;
        deny(
            record.parent != context.durable_task_id()
                || record.operation_id.into_bytes() == fork.caller.into_bytes()
                || record.operation_id == derived(context.operation_id(), "prepare", io)?,
            io,
        )?;
        let admission = 'admit: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                let (admission, custody) = runtime
                    .task_host()
                    .admit_owned_with_native_work(
                        record.clone_with_native_work(work)?,
                        journal_owner
                            .task_binding()
                            .1
                            .clone_with_native_work(work)?,
                        work,
                    )?
                    .await?;
                io.ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "native admission",
                    )
                })?
                .absorb_native_custody(custody)?;
                break 'admit admission;
            }
            break 'admit runtime
                .task_host()
                .admit_owned(record.clone(), journal_owner.task_binding().1.clone())
                .await?;
        };
        match admission {
            crate::Admission::Accepted(task) => deny(task != record.task_id(), io)?,
            crate::Admission::Indeterminate { .. } => return Ok(None),
            crate::Admission::Rejected { reason } => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    return Err(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Conflict,
                        message: reason,
                        custody: io.take_native_custody(io.owned_bytes()?)?,
                    });
                }
                return Err(Error::Conflict(reason));
            }
        }
        let before = io
            .map(super::super::PreparationIo::owned_bytes)
            .transpose()?;
        let same = runtime
            .admission_observed(fork.preparation.task_id(), io)
            .await?
            == fork.preparation;
        crate::filesystem::PreparationIo::release_since(io, before)?;
        deny(!same, io)?;
        let report = match runtime
            .outcome_observed(fork.preparation.task_id(), io)
            .await?
        {
            Some(Outcome::Succeeded(value)) => decode::<ForkReport>(&value, io)?,
            Some(Outcome::Failed { message }) => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    return Err(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Conflict,
                        message,
                        custody: io.take_native_custody(io.owned_bytes()?)?,
                    });
                }
                return Err(Error::Conflict(message));
            }
            Some(Outcome::Cancelled) => {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "preparation cancelled; child not admitted and allocation may remain uncertain",
                ));
            }
            None | Some(Outcome::Indeterminate { .. }) => return Ok(None),
        };
        crate::executor::with_history_io(io, |work| report.validate(work))?;
        deny(
            !recursive_encoded_equal(&report.request, &fork.request.payload, io)?,
            io,
        )?;
        let expected =
            crate::executor::with_history_io(io, |work| report.copy(work)?.0.into_seed(work))?;
        let copied = crate::executor::with_history_io(io, |work| {
            report.copy(work).map(|(report, _)| report)
        })?;
        let owned_io;
        let publication_io = if let Some(io) = io {
            io
        } else {
            owned_io = super::super::PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(
                    fork.request.maximum_work,
                    fork.request.maximum_bytes,
                ),
                cancellation,
            )?;
            &owned_io
        };
        let mut history = publication_io.history_io();
        binding
            .parent
            .publish_fork_report(
                copied,
                binding.parent_scope,
                Some(journal_owner.as_ref()),
                &mut Some(&mut history),
            )
            .await?;
        let seed = binding
            .parent
            .reducer()
            .fork(&report.request.child)
            .ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "acknowledged fork publication has no canonical seed",
                )
            })?;
        deny(seed != &expected, io)?;
        binding
            .child_conversation
            .bind_published_child(
                &binding.parent,
                seed,
                binding.child_scope,
                Some(journal_owner.as_ref()),
                &mut Some(&mut history),
            )
            .await?;
        let record = &fork.request.child;
        deny(
            record.parent != context.durable_task_id()
                || record.operation_id.into_bytes() == fork.caller.into_bytes()
                || record.operation_id == derived(context.operation_id(), "child-admit", io)?,
            io,
        )?;
        let admission = 'admit: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                let (admission, custody) = runtime
                    .task_host()
                    .admit_owned_with_native_work(
                        record.clone_with_native_work(work)?,
                        journal_owner
                            .task_binding()
                            .1
                            .clone_with_native_work(work)?,
                        work,
                    )?
                    .await?;
                io.ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "native admission",
                    )
                })?
                .absorb_native_custody(custody)?;
                break 'admit admission;
            }
            break 'admit runtime
                .task_host()
                .admit_owned(record.clone(), journal_owner.task_binding().1.clone())
                .await?;
        };
        match admission {
            crate::Admission::Accepted(task) => deny(task != record.task_id(), io)?,
            crate::Admission::Indeterminate { .. } => return Ok(None),
            crate::Admission::Rejected { reason } => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    return Err(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Conflict,
                        message: reason,
                        custody: io.take_native_custody(io.owned_bytes()?)?,
                    });
                }
                return Err(Error::Conflict(reason));
            }
        }
        #[derive(Serialize)]
        struct Admitted<'a> {
            status: &'static str,
            id: crate::AgentId,
            task: TaskId,
            source: TaskId,
            operation_id: OperationId,
            seed: &'a ForkSeed,
        }
        encode(
            &Admitted {
                status: "admitted",
                id: fork.arguments.id,
                task: fork.request.child.task_id(),
                source: fork.arguments.source.unwrap_or(fork.caller),
                operation_id: report.request.operation_id,
                seed,
            },
            output_io,
        )
        .map(Some)
    }
}
