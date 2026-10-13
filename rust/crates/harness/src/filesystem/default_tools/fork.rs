use super::*;
use crate::executor::stock_string_copy;

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn publish_fork(
        &self,
        context: &ToolContext,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<FilesystemWorkflowJournal<P, A, O>>,
        ),
        fork: &RecursiveForkToolPlan,
        report: Option<&ForkReport>,
        cancellation: &CancellationToken,
        output_io: Option<&super::super::PreparationIo>,
    ) -> Result<Value> {
        let (_, workflow) = owner;
        let journal_owner = workflow.owner.as_ref().expect("validated tool owner");
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
            )?
            .await?;
        validate_fork_binding(context, &fork.arguments, &binding, cancellation, io).await?;
        reject_if(
            !recursive_encoded_equal(&binding.source.request, &fork.request.payload, io)?
                || binding.child_message_pointer != fork.child_message_pointer
                || binding.child_message_file_pointer != fork.child_message_file_pointer
                || fork.request.source_checkpoint.as_ref() != Some(&binding.source_checkpoint),
            io,
            crate::wire::ErrorCode::Unauthorized,
            "restored fork source differs from original authenticated cut",
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
        reject_if(
            restored != fork.request,
            io,
            crate::wire::ErrorCode::Unauthorized,
            "restored child, source or allowances changed",
        )?;
        let report = report.ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "fork publication has no canonical report",
            )
        })?;
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
        reject_if(
            seed != &expected,
            io,
            crate::wire::ErrorCode::Unauthorized,
            "acknowledged fork seed changed original prepared report",
        )?;
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
        encode(seed, output_io)
    }
}
