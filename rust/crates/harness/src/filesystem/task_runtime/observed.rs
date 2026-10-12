//! Optional observation at the original finite worker's provider boundaries.

use super::*;
use crate::filesystem::{PreparationIo, fork_preparer_io::clone_text};

pub(in crate::filesystem) fn clone_fence(fence: &LeaseFence, io: Option<&PreparationIo>) -> Result<LeaseFence> {
    Ok(LeaseFence {
        reservation_id: clone_text(&fence.reservation_id, io)?,
        placement: clone_text(&fence.placement, io)?,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) fn clone_fence_with_custody(
    original: &LeaseFence, io: &PreparationIo,
) -> Result<(LeaseFence, acyclic_fs::NativeAllocation)> {
    io.retain_owned(std::mem::size_of::<LeaseFence>() as u64)?;
    let fence = clone_fence(original, Some(io))?;
    let bytes = (std::mem::size_of::<LeaseFence>() as u64)
        .checked_add(fence.reservation_id.capacity() as u64)
        .and_then(|bytes| bytes.checked_add(fence.placement.capacity() as u64))
        .ok_or_else(|| boundary(Some(io), crate::wire::ErrorCode::Invalid, "owned fence metadata overflow"))?;
    let custody = io.take_native_custody(bytes)?;
    Ok((fence, custody))
}

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
        payload: super::super::fork_preparer::bounded_io::clone_file(&command.payload, io)?,
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

pub(super) fn digest<T: serde::Serialize>(value: &T, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
    let Some(io) = io else { return crate::contract::canonical_json_digest(value); };
    let hold;
    let bytes = io.encode(value, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
    hold = io.guard_bytes(&bytes)?;
    let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
    io.preflight(hash)?;
    let digest = *blake3::hash(&bytes).as_bytes();
    io.charge(hash)?;
    drop(bytes);
    drop(hold);
    Ok(digest)
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
            return owner.verify_with_native_work(settlement, work).await;
        }
        if settlement { self.host.verify_execution_owner(task, fence.clone()).await }
        else { self.host.verify_dispatch_owner(task, fence.clone()).await }
    }

    pub(super) async fn settle_observed(&self, task: TaskId, fence: &LeaseFence, outcome: Outcome<Value>, io: Option<&PreparationIo>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "worker settlement requires original native work",
            })?;
            return self.host.settle_task_with_native_work(task, clone_fence(fence, Some(io))?, outcome, work).await;
        }
        self.host.settle_task(task, fence.clone(), outcome).await
    }

    pub(super) async fn suspend_observed(&self, task: TaskId, fence: &LeaseFence, revision: u64, io: Option<&PreparationIo>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "worker suspension requires original native work",
            })?;
            return self.host.suspend_workflow_with_native_work(task, clone_fence(fence, Some(io))?, revision, work).await;
        }
        self.host.suspend_workflow(task, fence.clone(), revision).await
    }
}
