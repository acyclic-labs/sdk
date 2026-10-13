//! Dispatch for the filesystem task runtime.

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
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<CommandDispatch> {
        if outbox.len() != 1
            && outbox
                .iter()
                .any(|command| command.kind == crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND)
        {
            return Err(Error::Invalid(
                "mail receive must checkpoint alone before the next command".into(),
            ));
        }
        let mut results = CommandDispatch::empty_results();
        for (index, command) in outbox.iter().enumerate() {
            self.owner_observed(task, fence, false, io).await?;
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
                    if !shared.as_ref().original_task_binding_matches(task, fence) {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unauthorized,
                            reason: "native command context differs from original task lease",
                        });
                    }
                    let body = shared
                        .as_ref()
                        .read_file_with_native_work(&command.payload, work)
                        .await?;
                    let payload = crate::filesystem::retain_decoded(io.decode(
                        std::marker::PhantomData::<Value>,
                        &body,
                        crate::wire::ErrorCode::Storage,
                    )?);
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
                        &context.read_file(&command.payload).await?,
                    )
                    .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
                    commands
                        .execute(context.clone(), fence.clone(), command.clone(), payload)
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
                    self.host
                        .verify_native_command_result(task, fence, custody.work())
                        .await?;
                }
                _ => self.owner_observed(task, fence, false, io).await?,
            }
            match progress {
                Ok(TaskCommandProgress::Pending) => {
                    return Ok(CommandDispatch::Pending(index));
                }
                Ok(TaskCommandProgress::Indeterminate) | Err(_) => {
                    return Ok(CommandDispatch::Indeterminate);
                }
                Ok(progress) => {
                    results.push_result(command.operation_id, progress)?;
                    if !results.input_within_limit(context.context().scope().limits().file_bytes)? {
                        return Ok(CommandDispatch::Indeterminate);
                    }
                }
            }
        }
        results.finish_results()?;
        Ok(results)
    }
}
