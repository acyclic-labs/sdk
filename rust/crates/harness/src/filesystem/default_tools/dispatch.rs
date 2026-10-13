//! Existing fenced task admission and operation-bound content APIs.
use super::*;
use crate::filesystem::{PreparationIo, PreparationRecord};
use crate::runtime::TaskSpawner as _;

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn admit(
        &self,
        context: &ToolContext,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<TaskJournalOwner<P>>,
        ),
        record: &TaskAdmissionRecord,
        command: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<TaskId>> {
        let caller = context.durable_task_id().expect("authorized caller");
        deny(
            record.parent != Some(caller)
                || record.operation_id.into_bytes() == caller.into_bytes()
                || record.operation_id == command,
            io,
        )?;
        let result = 'admit: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                let original = context.native_task().ok_or_else(|| {
                    recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "admission has no original task",
                    )
                })?;
                deny(
                    !original.original_task_binding_matches(caller, owner.1.task_binding().1),
                    io,
                )?;
                let (result, custody) = owner
                    .0
                    .task_host()
                    .admit_owned_with_native_work(
                        record.clone_with_native_work(work)?,
                        owner.1.task_binding().1.clone_with_native_work(work)?,
                        work,
                    )?
                    .await?;
                io.expect("native admission")
                    .absorb_native_custody(custody)?;
                break 'admit result;
            }
            owner
                .0
                .task_host()
                .admit_owned(record.clone(), owner.1.task_binding().1.clone())
                .await?
        };
        match result {
            crate::Admission::Accepted(task) => {
                deny(task != record.task_id(), io)?;
                Ok(Some(task))
            }
            crate::Admission::Indeterminate { .. } => Ok(None),
            crate::Admission::Rejected { reason } => Err(outcome_error(reason, io)?),
        }
    }
}

pub(super) fn outcome_error(message: String, io: Option<&PreparationIo>) -> Result<Error> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        return Ok(Error::NativeDiagnostic {
            code: crate::wire::ErrorCode::Conflict,
            message,
            custody: io.take_native_custody(io.owned_bytes()?)?,
        });
    }
    let _ = io;
    Ok(Error::Conflict(message))
}

pub(super) async fn retained_payload<T: PreparationRecord>(
    context: &ToolContext,
    file: &FileRef,
    maximum: u64,
    cancellation: &CancellationToken,
    io: Option<&PreparationIo>,
) -> Result<T> {
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.original_native_work() {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
    }
    reject_if(
        file.descriptor().byte_length()
            > maximum.min(context.effective_scope().limits().file_bytes),
        Some(io),
        crate::wire::ErrorCode::Invalid,
        "retained recursive payload exceeds its bound",
    )?;
    let bytes = context.read_file(file, Some(io)).await?;
    Ok(crate::filesystem::retain_decoded(io.decode(
        std::marker::PhantomData::<T>,
        &bytes,
        crate::wire::ErrorCode::Storage,
    )?))
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
        crate::executor::preparation_format(
            format_args!("recursive-tools/{}/{label}.json", context.operation_id()),
            work,
        )
    })?;
    path_hold = io.guard_owned(path.capacity() as u64)?;
    let display_hold;
    let display = crate::executor::with_history_io(Some(io), |work| {
        crate::executor::preparation_format(format_args!("{label}.json"), work)
    })?;
    display_hold = io.guard_owned(display.capacity() as u64)?;
    let result = context
        .stage_file_once(
            operation,
            &path,
            &bytes,
            "application/json",
            &display,
            Some(io),
        )
        .await
        .map(|(file, _)| file);
    drop((display, display_hold));
    drop((path, path_hold));
    result
}
