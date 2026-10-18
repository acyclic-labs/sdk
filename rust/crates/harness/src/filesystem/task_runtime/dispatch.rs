use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn dispatch_commands(
        &self,
        task: TaskId,
        context: &WorkerContext,
        fence: &LeaseFence,
        outbox: &[WorkflowCommand],
        commands: &dyn TaskCommandHost,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<CommandDispatch> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if outbox.len() != 1
                && outbox
                    .iter()
                    .any(|command| command.kind == super::super::MAIL_RECEIVE_TASK_COMMAND_KIND)
            {
                return Err(Error::Invalid(
                    "mail receive must checkpoint alone before the next command".into(),
                ));
            }
            let mut results = CommandDispatch::empty_results();
            let owner = context.context().owned_mailbox.as_ref();
            for command in outbox {
                match owner {
                    Some(owner) => owner.verify_owner(io)?.await?,
                    None => self.owner_observed(task, fence, false, io).await?,
                }
                if command.payload.descriptor().byte_length() > self.maximum_payload_bytes {
                    #[cfg(not(target_arch = "wasm32"))]
                    if io.is_some() {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Invalid,
                            reason: "command exceeds payload limit",
                        });
                    }
                    return Err(Error::Invalid("command exceeds payload limit".into()));
                }
                let progress = match context {
                    #[cfg(not(target_arch = "wasm32"))]
                    WorkerContext::Native(shared) => {
                        let io = io.ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "native dispatch lost its original observer",
                        })?;
                        let work = io.original_native_work().ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "native dispatch requires original work",
                        })?;
                        if !shared.original_task_binding_matches(task, fence) {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unauthorized,
                                reason: "native command context differs from original task lease",
                            });
                        }
                        let body = shared
                            .read_file(
                                &command.payload,
                                Some(&crate::executor::PreparationIo::from_native_work(work)?),
                            )
                            .await?;
                        let payload = crate::filesystem::retain_decoded(io.decode(
                            std::marker::PhantomData::<Value>,
                            &body,
                            crate::wire::ErrorCode::Storage,
                        )?)?;
                        drop(body);
                        commands
                            .execute_with_native_work(
                                Arc::clone(shared),
                                crate::executor::with_history_io(Some(io), |work| {
                                    fence.clone_with_work(work)
                                })?,
                                observed::clone_command(command, Some(io))?,
                                payload,
                                work,
                            )?
                            .await
                    }
                    WorkerContext::Ordinary(context) => {
                        let payload = crate::contract::json_from_slice(
                            &context.read_file(&command.payload, None).await?,
                        )
                        .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
                        commands
                            .execute(
                                context.as_ref().clone(),
                                fence.clone(),
                                command.clone(),
                                payload,
                            )
                            .await
                    }
                };
                #[cfg(not(target_arch = "wasm32"))]
                let progress = match progress {
                    Err(error) if error.is_native_failure() => return Err(error),
                    progress => progress,
                };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    match &progress {
                        Ok(TaskCommandProgress::Ready(_)) => {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unsupported,
                                reason: "native command returned an unobserved ordinary ready value",
                            });
                        }
                        Ok(TaskCommandProgress::ReadyWithNativeCustody { custody, .. }) => {
                            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unsupported,
                                reason: "native result lost its original observer",
                            })?;
                            if !work.shares_ledger(custody.work()) {
                                return Err(Error::NativeBoundary {
                                    code: crate::wire::ErrorCode::Unauthorized,
                                    reason: "native command result differs from original work",
                                });
                            }
                        }
                        _ => {}
                    }
                }
                match &progress {
                    #[cfg(not(target_arch = "wasm32"))]
                    Ok(TaskCommandProgress::ReadyWithNativeCustody { custody, .. }) => {
                        let io = crate::executor::PreparationIo::from_native_work(custody.work())?;
                        match owner {
                            Some(owner) => owner.verify_owner(Some(&io))?.await?,
                            None => self.owner_observed(task, fence, false, Some(&io)).await?,
                        }
                    }
                    _ => match owner {
                        Some(owner) => owner.verify_owner(io)?.await?,
                        None => self.owner_observed(task, fence, false, io).await?,
                    },
                }
                match progress {
                    Ok(TaskCommandProgress::Pending) => {
                        return Ok(CommandDispatch::Pending {
                            command: command.operation_id,
                            guard_execution: matches!(
                                command.kind.as_str(),
                                super::super::TOOL_TASK_COMMAND_KIND
                                    | super::super::MODEL_TASK_COMMAND_KIND
                            ),
                        });
                    }
                    Err(error @ Error::InteractionRejected(crate::InteractionRejection::Cancelled)) => {
                        return Err(error);
                    }
                    Err(error) => {
                        let reason = crate::executor::with_history_io(io, |work| -> Result<_> {
                            Ok(crate::CommandIndeterminacyReason::Failed {
                                code: crate::executor::preparation_format(
                                    format_args!("{}", error.kind()),
                                    work,
                                )?,
                                error: crate::executor::preparation_format(
                                    format_args!("{error}"),
                                    work,
                                )?,
                            })
                        })?;
                        return command_indeterminate(task, command, reason, io);
                    }
                    Ok(TaskCommandProgress::Indeterminate) => {
                        return command_indeterminate(
                            task,
                            command,
                            crate::CommandIndeterminacyReason::Unsettled,
                            io,
                        );
                    }
                    Ok(progress) => {
                        results.push_result(command.operation_id, progress)?;
                        if !results.input_within_limit(context.context().scope().limits().file_bytes)? {
                            return command_indeterminate(
                                task,
                                command,
                                crate::CommandIndeterminacyReason::InputLimit,
                                io,
                            );
                        }
                    }
                }
            }
            results.finish_results()?;
            Ok(results)
        })?
        .await
    }
}

/// Records why a command left its task uncertain, so the cause reaches the
/// task's outcome, the caller's error and the logs instead of being erased.
fn command_indeterminate(
    task: TaskId,
    command: &WorkflowCommand,
    reason: crate::CommandIndeterminacyReason,
    io: Option<&super::super::PreparationIo>,
) -> Result<CommandDispatch> {
    let cause = crate::CommandIndeterminacy {
        command: crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", command.kind), work)
        })?,
        reason,
    };
    tracing::warn!(
        %task,
        operation = %command.operation_id,
        %cause,
        "task command left its task indeterminate"
    );
    Ok(CommandDispatch::Indeterminate(cause))
}
