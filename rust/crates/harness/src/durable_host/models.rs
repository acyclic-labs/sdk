//! Models.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    #[cfg(feature = "filesystem")]
    pub(crate) async fn verify_model_history<'a>(
        &self,
        task: TaskId,
        operation: OperationId,
        records: impl Iterator<Item = &'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if !records.clone().any(|record| {
            matches!(
                record.event,
                crate::executor::ExecutionEvent::ModelStarted { .. }
            )
        }) {
            return Ok(());
        }
        let task_operation = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            task_operation,
            work,
        )?;
        for record in records {
            if let crate::executor::ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } = &record.event
            {
                coordinator.scheduler().require_model_claim(
                    task_operation,
                    purpose.attempt_operation(operation, None)?,
                    *step,
                    request_digest,
                    work,
                )?;
            }
        }
        Ok(())
    }

    pub(super) async fn claim_model_dispatch_with_work(
        &self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: &crate::scheduler::LeaseFence,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        let operation_id = OperationId::from_bytes(task_id.into_bytes());
        let admission = self
            .read_retained_admission_counted(operation_id, work)
            .await
            .map(|(record, _)| record)?;
        let (scope, _) = self.authenticated_scope_with_work(&admission, work).await?;
        if !admission.grants.contains("model:generate") {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks model:generate",
            ));
        }
        self.validate_parent_scope_with_work(admission.parent, &scope, work)
            .await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        crate::scheduler::require_execution_owner(operation, fence, false, work)?;
        let session = coordinator.scheduler().session_limits(operation_id, work)?;
        let ceiling = (scope.limits().model_steps as u64)
            .min(
                scope
                    .run_limits()
                    .max_steps
                    .map_or(u64::MAX, |value| value as u64),
            )
            .min(session.model_steps);
        let lease_digest = crate::contract::canonical_json_digest(
            fence,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let mut uuid = [0; 36];
        let attempt = uuid::Uuid::from_bytes(attempt_id.into_bytes())
            .hyphenated()
            .encode_lower(&mut uuid);
        let digits = if step == 0 {
            1
        } else {
            step.ilog10() as usize + 1
        };
        let length = "model-dispatch:".len() + attempt.len() + 1 + digits + 1 + 64;
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(length as u64)?;
            io.reserve_text(&mut key, length)?;
            io.copy_bytes(length as u64)?;
        } else {
            key.reserve_exact(length);
        }
        use std::fmt::Write as _;
        write!(
            &mut key,
            "model-dispatch:{attempt}:{step}:{}",
            blake3::Hash::from(lease_digest)
        )
        .map_err(|_| {
            admission_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "model dispatch key formatting failed",
            )
        })?;
        let fence = fence.clone_with_work(work)?;
        coordinator
            .apply(
                IdempotencyKey::new(key)?,
                crate::scheduler::SchedulerEvent::ModelDispatchClaimed {
                    operation_id,
                    attempt_id,
                    step,
                    request_digest,
                    fence,
                    ceiling,
                },
                work,
            )
            .await?;
        Ok(())
    }
}
