//! Dispatch for portable recursive tools.

use super::*;

impl<P, A, O> RecursiveExecutor<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn dispatch(
        &self,
        context: &ToolContext,
        owner: &RecursiveToolOwner<P, A, O>,
        plan: &Plan,
        stage: &str,
        command: &WorkflowCommand,
        payload: Value,
        report: Option<&ForkReport>,
        receipt: Option<&ProjectMergeReceipt>,
        cancellation: &CancellationToken,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Option<Value>> {
        #[cfg(not(target_arch = "wasm32"))]
        let native_work = io.and_then(crate::filesystem::PreparationIo::original_native_work);
        let outcome = match (plan, stage) {
            (Plan::Fork(_), "prepare" | "child-admit") => {
                let progress = 'progress: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = native_work {
                        let context = context.native_task().ok_or_else(|| {
                            recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "recursive command has no original native task context",
                            )
                        })?;
                        let temporary_io =
                            crate::filesystem::PreparationIo::from_native_work(work)?;
                        let history = &mut temporary_io.history_io();
                        let original_fence = owner.journal_owner.task_binding().1;
                        let fence = crate::scheduler::LeaseFence {
                            reservation_id: history.clone_text(&original_fence.reservation_id)?,
                            placement: history.clone_text(&original_fence.placement)?,
                        };
                        let command = WorkflowCommand {
                            operation_id: command.operation_id,
                            kind: history.clone_text(&command.kind)?,
                            payload: command.payload.copy(&mut Some(&mut *history))?,
                        };
                        let payload = crate::wire_codec::CodecQuota::produce(
                            &mut Some(&mut *history),
                            |copy_quota| crate::core::copy_value(&payload, copy_quota),
                        )?
                        .0;
                        break 'progress owner
                            .runtime
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
                    break 'progress owner
                        .runtime
                        .commands()
                        .execute(
                            context.task().clone(),
                            owner.journal_owner.task_binding().1.clone(),
                            command.clone(),
                            payload,
                        )
                        .await?;
                };
                match progress {
                    TaskCommandProgress::Ready(value) => {
                        if io.is_some_and(|io| io.original_native_work().is_some()) {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "native recursive command reply lacks its original payload custody",
                            ));
                        }
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
                if owner
                    .runtime
                    .admission_observed(fork.preparation.task_id(), io)
                    .await?
                    != fork.preparation
                {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "observed preparation admission changed",
                    ));
                }
                let outcome = owner
                    .runtime
                    .outcome_observed(fork.preparation.task_id(), io)
                    .await?;
                if let Some(Outcome::Succeeded(value)) = &outcome {
                    recursive_decoded::<ForkReport, _>(value, io, |report| {
                        crate::executor::with_history_io(io, |work| report.validate(work))?;
                        if !recursive_encoded_equal(&report.request, &fork.request.payload, io)? {
                            return Err(recursive_reject(
                                io,
                                crate::wire::ErrorCode::Unauthorized,
                                "preparation output changed original retained request",
                            ));
                        }
                        Ok(())
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
                    "recursive command does not match its retained plan",
                ));
            }
        };
        match outcome {
            Some(Outcome::Succeeded(value)) => Ok(Some(value)),
            Some(Outcome::Failed { message }) => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
                    let custody = io.take_native_custody(io.owned_bytes()?)?;
                    return Err(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Conflict,
                        message,
                        custody,
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

pub(super) fn command_payload(
    stage: &str,
    plan: &Plan,
    report: Option<&ForkReport>,
    receipt: Option<&ProjectMergeReceipt>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Value> {
    #[derive(Serialize)]
    struct Admission<'a> {
        admission: &'a TaskAdmissionRecord,
    }
    #[derive(Serialize)]
    struct Observation {
        preparation: TaskId,
    }
    match (plan, stage) {
        (Plan::Fork(plan), "prepare") => encode(
            &Admission {
                admission: &plan.preparation,
            },
            io,
        ),
        (Plan::Fork(plan), "observe") => encode(
            &Observation {
                preparation: plan.preparation.task_id(),
            },
            io,
        ),
        (Plan::Fork(_), "fork-publish") => encode(
            report.ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "publication report missing",
                )
            })?,
            io,
        ),
        (Plan::Fork(plan), "child-admit") => encode(
            &Admission {
                admission: &plan.request.child,
            },
            io,
        ),
        (Plan::Merge(plan), "merge-apply") => encode(plan, io),
        (Plan::Merge(_), "merge-publish") => encode(
            receipt.ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "publication receipt missing",
                )
            })?,
            io,
        ),
        _ => Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Invalid,
            "command payload does not match retained plan",
        )),
    }
}

pub(super) async fn retained_payload(
    context: &ToolContext,
    file: &FileRef,
    maximum: u64,
    cancellation: &CancellationToken,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Value> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let work = io
            .original_native_work()
            .expect("native payload reads preserve their original ledger");
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        if file.descriptor().byte_length()
            > maximum.min(context.effective_scope().limits().file_bytes)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "retained recursive payload exceeds its original bound",
            });
        }
        let bytes = context.read_file(file, Some(&crate::executor::PreparationIo::from_native_work(work)?)).await?;
        return Ok(crate::filesystem::retain_decoded(io.decode(
            std::marker::PhantomData::<Value>,
            &bytes,
            crate::wire::ErrorCode::Storage,
        )?));
    }
    let ordinary;
    let content_io = match io {
        Some(io) => io,
        None => {
            ordinary = crate::filesystem::PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(maximum, maximum), cancellation,
            )?;
            &ordinary
        }
    };
    let bytes = context.read_file(file, Some(content_io)).await?;
    crate::contract::json_from_slice(&bytes).map_err(|error| Error::Storage(error.to_string()))
}
pub(super) async fn stage(
    context: &ToolContext,
    operation: OperationId,
    label: &str,
    value: &Value,
    maximum: u64,
    cancellation: &CancellationToken,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<FileRef> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let work = io
            .original_native_work()
            .expect("native staging preserves its original ledger");
        let bytes = io.encode(
            value,
            maximum.min(context.effective_scope().limits().file_bytes),
            true,
        )?;
        let _path;
        let path = crate::executor::with_history_io(Some(io), |work| crate::contract::preparation_format(format_args!("recursive-tools/{}/{label}.json", context.operation_id()), work))?;
        _path = io.guard_owned(path.capacity() as u64)?;
        let _display;
        let display = crate::executor::with_history_io(Some(io), |work| crate::contract::preparation_format(format_args!("{label}.json"), work))?;
        _display = io.guard_owned(display.capacity() as u64)?;
        let (file, custody) = {
            let publication_io = crate::executor::PreparationIo::from_native_work(work)?;
            context
                .stage_file_once(
                    operation,
                    &path,
                    &bytes,
                    "application/json",
                    &display,
                    Some(&publication_io),
                )
                .await
                .and_then(|(file, retained)| Ok((file, publication_io.take_native_custody(retained)?)))
        }?;
        io.absorb_native_custody(custody)?;
        return Ok(file);
    }
    let _ = io;
    let io = crate::filesystem::fork_preparer::PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(maximum, maximum),
        cancellation,
    )?;
    let bytes = io.encode(
        value,
        maximum.min(context.scope().limits().file_bytes),
        true,
    )?;
    context
        .stage_file_once(
            operation,
            &format!("recursive-tools/{}/{label}.json", context.operation_id()),
            &bytes,
            "application/json",
            &format!("{label}.json"),
            Some(&io),
        )
        .await
        .map(|(file, _)| file)

}
