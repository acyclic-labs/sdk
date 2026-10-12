//! Optional observation at the original finite worker's provider boundaries.

use super::*;
use crate::filesystem::{PreparationIo, fork_preparer_io::clone_text};

pub(super) fn lease_fence(lease: &crate::distributed::WorkLease, io: Option<&PreparationIo>) -> Result<LeaseFence> {
    Ok(LeaseFence {
        reservation_id: clone_text(&lease.reservation.id, io)?,
        placement: clone_text(&lease.reservation.placement, io)?,
    })
}

pub(super) fn clone_command(command: &WorkflowCommand, io: Option<&PreparationIo>) -> Result<WorkflowCommand> {
    Ok(WorkflowCommand {
        operation_id: command.operation_id,
        kind: clone_text(&command.kind, io)?,
        payload: crate::executor::with_history_io(io, |work| command.payload.copy(work))?,
    })
}

pub(super) fn pull_has_native_failure(pull: &crate::distributed::WorkPull) -> bool {
    #[cfg(not(target_arch = "wasm32"))]
    if let crate::distributed::WorkPull::Unresolved { error, .. } = pull {
        return error.is_native_failure();
    }
    let _ = pull;
    false
}

pub(in crate::filesystem) fn boundary(io: Option<&PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.is_some() { return Error::NativeBoundary { code, reason }; }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(super) fn history<'a>(io: Option<&'a PreparationIo>) -> Result<Option<impl crate::store::HistoryIo + 'a>> {
    #[cfg(not(target_arch = "wasm32"))]
    let history = io.map(|io| {
        let work = io.original_native_work().ok_or_else(|| boundary(Some(io),
            crate::wire::ErrorCode::Unsupported, "worker observation requires original native work"))?;
        crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)
    }).transpose()?;
    #[cfg(target_arch = "wasm32")]
    let history = { let _ = io; None::<&PreparationIo> };
    Ok(history)
}

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn admission_observed(&self, task: TaskId, io: Option<&PreparationIo>) -> Result<TaskAdmissionRecord> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "worker observation requires original native work",
            })?;
            let (admission, custody) = self.host.observe_admission_with_native_work(task, work).await?;
            io.absorb_native_custody(custody)?;
            return Ok(admission);
        }
        self.host.observe_admission(task).await
    }

    pub(in crate::filesystem) async fn outcome_observed(&self, task: TaskId, io: Option<&PreparationIo>) -> Result<Option<Outcome<Value>>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "worker outcome requires original native work",
            })?;
            let (outcome, custody) = self.host.outcome_with_native_work(task, work).await?;
            io.absorb_native_custody(custody)?;
            return Ok(outcome);
        }
        self.host.outcome(task).await
    }

    pub(in crate::filesystem) async fn owner_observed(&self, task: TaskId, fence: &LeaseFence, settlement: bool, io: Option<&PreparationIo>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "worker owner requires original native work",
            })?;
            let owner = self.host.journal_owner_with_native_work(task, fence, work).await?;
            return owner.verify(settlement, &mut Some(&mut crate::store::HistoryWork::new(
                        crate::filesystem::PreparationIo::from_native_work(work)?,
                    ))).await;
        }
        if settlement { self.host.verify_execution_owner(task, fence.clone()).await }
        else { self.host.verify_dispatch_owner(task, fence.clone()).await }
    }

}
