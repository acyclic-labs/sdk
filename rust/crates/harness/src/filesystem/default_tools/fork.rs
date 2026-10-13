use super::*;
use crate::executor::stock_string_copy;

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn run_fork(
        &self,
        context: &ToolContext,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<TaskJournalOwner<P>>,
        ),
        fork: &RecursiveForkToolPlan,
        cancellation: &CancellationToken,
        output_io: Option<&super::super::PreparationIo>,
    ) -> Result<Option<Value>> {
        let (runtime, journal_owner) = owner;
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
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
            .expect("recursive source was authorized")
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
        if self
            .admit(
                context,
                owner,
                &fork.preparation,
                derived(context.operation_id(), "prepare", io)?,
                io,
            )
            .await?
            .is_none()
        {
            return Ok(None);
        }
        let before = io
            .map(super::super::PreparationIo::owned_bytes)
            .transpose()?;
        let same = runtime
            .admission_observed(fork.preparation.task_id(), io)
            .await?
            == fork.preparation;
        recursive_release_since(io, before)?;
        deny(!same, io)?;
        let report = match runtime
            .outcome_observed(fork.preparation.task_id(), io)
            .await?
        {
            Some(Outcome::Succeeded(value)) => decode::<ForkReport>(&value, io)?,
            Some(Outcome::Failed { message }) => return Err(outcome_error(message, io)?),
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
        let publication_io = match io {
            Some(io) => io,
            None => {
                owned_io = super::super::PreparationIo::from_budget(
                    acyclic_fs::performance::work_budget(
                        fork.request.maximum_work,
                        fork.request.maximum_bytes,
                    ),
                    cancellation,
                )?;
                &owned_io
            }
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
        if self
            .admit(
                context,
                owner,
                &fork.request.child,
                derived(context.operation_id(), "child-admit", io)?,
                io,
            )
            .await?
            .is_none()
        {
            return Ok(None);
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
