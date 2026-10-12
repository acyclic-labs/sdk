//! Fork for portable recursive tools.

use super::*;

impl<P, A, O> RecursiveExecutor<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn publish_fork(
        &self,
        context: &ToolContext,
        owner: &RecursiveToolOwner<P, A, O>,
        fork: &RecursiveForkToolPlan,
        report: Option<&ForkReport>,
        cancellation: &CancellationToken,
        output_io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Value> {
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
        if !recursive_encoded_equal(&binding.source.request, &fork.request.payload, io)?
            || binding.child_message_pointer != fork.child_message_pointer
            || binding.child_message_file_pointer != fork.child_message_file_pointer
            || fork.request.source_checkpoint.as_ref() != Some(&binding.source_checkpoint)
        {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "restored fork source differs from original authenticated cut",
            ));
        }
        let command = fork.request.command.as_ref().ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "original preparation command missing",
            )
        })?;
        let command = WorkflowCommand {
            operation_id: command.operation_id,
            kind: stock_string_copy(&command.kind, io)?,
            payload: crate::executor::with_history_io(io, |work| command.payload.copy(work))?,
        };
        let restored = fork_preparation_request(
            binding.source.preparer.as_ref(),
            &binding.source.request,
            fork.preparation.task_id(),
            binding.child,
            command,
            binding.limits,
            Some(binding.source_checkpoint),
            binding.input_files,
            io,
        )?;
        if restored != fork.request {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "restored child, source or allowances changed",
            ));
        }
        let report = report.ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "fork publication has no canonical report",
            )
        })?;
        let expected_seed =
            crate::executor::with_history_io(io, |work| report.copy(work)?.0.into_seed(work))?;
        let report_copy = crate::executor::with_history_io(io, |work| {
            report.copy(work).map(|(report, _)| report)
        })?;
        let remaining = 'publication: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                binding
                    .parent
                    .publish_fork_report(
                        report_copy,
                        binding.parent_scope,
                        Some(owner.journal_owner.as_ref()),
                        &mut Some(&mut io.history_io()),
                    )
                    .await?;
                break 'publication None;
            }
            let budget = acyclic_fs::performance::work_budget(
                fork.request.maximum_work,
                fork.request.maximum_bytes,
            );
            let mut publication_work = crate::store::HistoryWork::new(budget, cancellation);
            let published = async {
                publication_work.preflight()?;
                binding
                    .parent
                    .publish_fork_report(
                        report_copy,
                        binding.parent_scope,
                        Some(owner.journal_owner.as_ref()),
                        &mut Some(&mut publication_work),
                    )
                    .await
            }
            .await;
            let published = publication_work
                .finish(published)
                .map_err(|failure| failure.error)?;
            break 'publication Some(
                published
                    .work
                    .remaining(budget)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
        };
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
        if seed != &expected_seed {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "acknowledged fork seed changed original prepared report",
            ));
        }
        'child: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                binding
                    .child_conversation
                    .bind_published_child(
                        &binding.parent,
                        seed,
                        binding.child_scope,
                        Some(owner.journal_owner.as_ref()),
                        &mut Some(&mut io.history_io()),
                    )
                    .await?;
                break 'child;
            }
            let mut child_work = crate::store::HistoryWork::new(
                remaining.expect("measured publication preserves its remaining allowance"),
                cancellation,
            );
            let bound = async {
                child_work.preflight()?;
                binding
                    .child_conversation
                    .bind_published_child(
                        &binding.parent,
                        seed,
                        binding.child_scope,
                        Some(owner.journal_owner.as_ref()),
                        &mut Some(&mut child_work),
                    )
                    .await
            }
            .await;
            child_work.finish(bound).map_err(|failure| failure.error)?;
        }
        encode(seed, output_io)
    }
}

use crate::executor::{stock_string_copy};
