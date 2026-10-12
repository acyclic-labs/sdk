//! Deadline cancellation for the original admitted task.
use super::*;
use crate::store::guarded_boundary;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Reads the original owner's current cancellation flag.
    pub async fn cancellation_requested(
        &self,
        task: TaskId,
        native: crate::NativeWork<'_>,
    ) -> Result<bool> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = native
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        let _ = native;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
            &mut work,
        )?;
        Ok(operation.cancellation_requested)
    }

    /// Cancels when the original admitted deadline has elapsed.
    pub async fn expire_task_deadline(
        &self,
        task: TaskId,
        fence: Option<&crate::scheduler::LeaseFence>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work.as_deref().and_then(|work| work.original_native_work())
            .map(crate::filesystem::PreparationIo::from_native_work).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        let admission = self.admission(operation, io.as_ref()).await?;
        let deadline = admission.run_limits.deadline_epoch_ms.ok_or_else(|| {
            guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "task has no admitted deadline",
            )
        })?;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.clock.now_unix_millis() < deadline {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "task deadline has not elapsed",
            ));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let state = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation,
            work,
        )?;
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| !matches!(outcome, Outcome::Indeterminate { .. }))
        {
            return Ok(());
        }
        if let Some(fence) = fence {
            if state.reservation.as_ref().is_none_or(|reservation| {
                reservation.id != fence.reservation_id || reservation.placement != fence.placement
            }) {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "deadline observer does not own the current reservation",
                ));
            }
        }
        if state.cancellation_requested {
            return Ok(());
        }
        let mut text = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(48)?;
            io.reserve_text(&mut text, 48)?;
            io.copy_bytes(48)?;
        }
        use std::fmt::Write as _;
        write!(&mut text, "task-cancel:{operation}").expect("writing into String");
        coordinator
            .cancel_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation,
                IdempotencyKey::new(text)?,
                true,
                None,
                work,
            )
            .await?;
        Ok(())
    }
}
