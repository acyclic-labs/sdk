//! Typed task-command dispatch and immutable protocol payloads.
use super::*;
use crate::filesystem::PreparationIo;

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn dispatch(
        &self,
        context: &ToolContext,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<FilesystemWorkflowJournal<P, A, O>>,
        ),
        plan: &Plan,
        stage: &str,
        command: &WorkflowCommand,
        report: Option<&ForkReport>,
        receipt: Option<&ProjectMergeReceipt>,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Value>> {
        let (runtime, workflow) = owner;
        let owner_guard = workflow.owner.as_ref().expect("validated tool owner");
        let outcome = match (plan, stage) {
            (Plan::Fork(fork), "prepare" | "child-admit") => {
                let payload = retained_payload(
                    context,
                    &command.payload,
                    self.maximum_request_bytes,
                    cancellation,
                    io,
                )
                .await?;
                let admission = if stage == "prepare" {
                    &fork.preparation
                } else {
                    &fork.request.child
                };
                let actual = payload.get("admission").ok_or_else(|| {
                    recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "command has no admission",
                    )
                })?;
                reject_if(
                    payload.as_object().is_none_or(|object| object.len() != 1)
                        || !recursive_encoded_equal(admission, actual, io)?,
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "command changed original admission",
                )?;
                let progress = 'execute: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                        let context = context.native_task().ok_or_else(|| {
                            recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "native command has no original task",
                            )
                        })?;
                        let temporary = PreparationIo::from_native_work(work)?;
                        let fence = owner_guard.task_binding().1;
                        let fence = crate::scheduler::LeaseFence {
                            reservation_id: crate::executor::stock_string_copy(
                                &fence.reservation_id,
                                Some(&temporary),
                            )?,
                            placement: crate::executor::stock_string_copy(
                                &fence.placement,
                                Some(&temporary),
                            )?,
                        };
                        let command = crate::filesystem::task_runtime::observed::clone_command(
                            command,
                            Some(&temporary),
                        )?;
                        let payload = crate::executor::with_history_io(Some(&temporary), |work| {
                            crate::wire_codec::CodecQuota::produce(work, |quota| {
                                crate::core::copy_value(&payload, quota)
                            })
                        })?
                        .0;
                        break 'execute runtime
                            .commands()
                            .execute_with_native_work(
                                Arc::clone(context),
                                fence,
                                command,
                                payload,
                                work,
                            )?
                            .await?;
                    }
                    break 'execute runtime
                        .commands()
                        .execute(
                            context.task().clone(),
                            owner_guard.task_binding().1.clone(),
                            command.clone(),
                            payload,
                        )
                        .await?;
                };
                match progress {
                    TaskCommandProgress::Ready(value) => {
                        reject_if(
                            io.is_some_and(|io| io.original_native_work().is_some()),
                            io,
                            crate::wire::ErrorCode::Unauthorized,
                            "native command result has no original guard",
                        )?;
                        Some(decode::<Outcome<Value>>(&value, io)?)
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    TaskCommandProgress::ReadyWithNativeCustody { value, custody } => {
                        let retained = (value, custody);
                        Some(decode::<Outcome<Value>>(&retained.0, io)?)
                    }
                    TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => None,
                }
            }
            (Plan::Fork(fork), "observe") => {
                let before = io.map(PreparationIo::owned_bytes).transpose()?;
                let same = runtime
                    .admission_observed(fork.preparation.task_id(), io)
                    .await?
                    == fork.preparation;
                recursive_release_since(io, before)?;
                reject_if(
                    !same,
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "preparation admission changed",
                )?;
                let outcome = runtime
                    .outcome_observed(fork.preparation.task_id(), io)
                    .await?;
                if let Some(Outcome::Succeeded(value)) = &outcome {
                    recursive_decoded::<ForkReport, _>(value, io, |report| {
                        crate::executor::with_history_io(io, |work| report.validate(work))?;
                        reject_if(
                            !recursive_encoded_equal(&report.request, &fork.request.payload, io)?,
                            io,
                            crate::wire::ErrorCode::Unauthorized,
                            "preparation report changed its request",
                        )
                    })?;
                }
                outcome
            }
            (Plan::Fork(fork), "fork-publish") => {
                return self
                    .publish_fork(context, owner, fork, report, cancellation, io)
                    .await
                    .map(Some);
            }
            (Plan::Merge(merge), "merge-apply") => {
                return self
                    .apply_merge(context, owner, merge, cancellation, io)
                    .await
                    .map(Some);
            }
            (Plan::Merge(merge), "merge-publish") => {
                return self
                    .publish_merge(context, owner, merge, receipt, cancellation, io)
                    .await
                    .map(Some);
            }
            _ => {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "command does not match its retained plan",
                ));
            }
        };
        match outcome {
            Some(Outcome::Succeeded(value)) => Ok(Some(value)),
            Some(Outcome::Failed { message }) => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
                    return Err(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Conflict,
                        message,
                        custody: io.take_native_custody(io.owned_bytes()?)?,
                    });
                }
                Err(Error::Conflict(message))
            }
            Some(Outcome::Cancelled) => Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Conflict,
                if stage == "observe" {
                    "preparation cancelled; child not admitted and allocation may remain uncertain"
                } else {
                    "recursive admission cancelled"
                },
            )),
            None | Some(Outcome::Indeterminate { .. }) => Ok(None),
        }
    }
}

pub(super) async fn retained_payload(
    context: &ToolContext,
    file: &FileRef,
    maximum: u64,
    cancellation: &CancellationToken,
    io: Option<&PreparationIo>,
) -> Result<Value> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        reject_if(
            file.descriptor().byte_length()
                > maximum.min(context.effective_scope().limits().file_bytes),
            io,
            crate::wire::ErrorCode::Invalid,
            "retained recursive payload exceeds its bound",
        )?;
        let bytes = context.read_file_with_native_work(file, work).await?;
        return Ok(crate::filesystem::retain_decoded(
            io.expect("native payload").decode(
                std::marker::PhantomData::<Value>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?,
        ));
    }
    let bytes = context
        .task()
        .read_file_measured(
            file,
            acyclic_fs::performance::work_budget(maximum, maximum),
            cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    crate::contract::json_from_slice(&bytes).map_err(|error| Error::Storage(error.to_string()))
}
pub(super) async fn stage<T: Serialize>(
    context: &ToolContext,
    operation: OperationId,
    label: &str,
    value: &T,
    maximum: u64,
    cancellation: &CancellationToken,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let ordinary;
    let io = match io {
        Some(io) => io,
        None => {
            ordinary = PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(maximum, maximum),
                cancellation,
            )?;
            &ordinary
        }
    };
    let bytes = io.encode(
        value,
        maximum.min(context.effective_scope().limits().file_bytes),
        true,
    )?;
    let path_hold;
    let path = crate::executor::with_history_io(Some(io), |work| {
        crate::contract::preparation_format(
            format_args!("recursive-tools/{}/{label}.json", context.operation_id()),
            work,
        )
    })?;
    path_hold = io.guard_owned(path.capacity() as u64)?;
    let display_hold;
    let display = crate::executor::with_history_io(Some(io), |work| {
        crate::contract::preparation_format(format_args!("{label}.json"), work)
    })?;
    display_hold = io.guard_owned(display.capacity() as u64)?;
    let result = 'stage: {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            let (file, custody) = context
                .stage_file_once_with_native_work(
                    operation,
                    &path,
                    &bytes,
                    "application/json",
                    &display,
                    work,
                )
                .await?;
            io.absorb_native_custody(custody)?;
            break 'stage Ok(file);
        }
        io.receive_content(
            context
                .stage_file_once_measured(
                    operation,
                    &path,
                    &bytes,
                    "application/json",
                    &display,
                    io.remaining()?,
                    cancellation,
                )
                .await,
        )
    };
    drop((display, display_hold));
    drop((path, path_hold));
    result
}
