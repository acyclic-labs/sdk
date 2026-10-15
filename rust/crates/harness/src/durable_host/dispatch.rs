//! Retained model dispatch authorization.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn claim_model(
        &self,
        task: TaskId,
        attempt: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: &crate::scheduler::LeaseFence,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id, quota.work()).await?;
        if quota.work().is_some() {
            quota.adopt(
                std::mem::size_of::<TaskAdmissionRecord>() as u64
                    + admission.logical_owned_bytes()?,
            )?;
        }
        if !admission.grants.contains("model:generate") {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks model:generate",
            ));
        }
        let scope = RuntimeScope::new(admission.grants, admission.limits)?
            .with_run_limits(admission.run_limits, quota.work())?;
        self.validate_parent_scope(admission.parent, &scope, quota.work())
            .await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(quota.work()).await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            quota.work(),
        )?;
        crate::scheduler::require_execution_owner(operation, fence, false, quota.work())?;
        let session = coordinator
            .scheduler()
            .session_limits(operation_id, quota.work())?;
        let ceiling = (scope.limits().model_steps as u64)
            .min(
                scope
                    .run_limits()
                    .max_steps
                    .map_or(u64::MAX, |limit| limit as u64),
            )
            .min(session.model_steps);
        let digest = crate::executor::json::canonical_json_digest(
            fence,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            quota.work(),
        )?;
        let key = crate::executor::preparation_format(
            format_args!(
                "model-dispatch:{attempt}:{step}:{}",
                blake3::Hash::from_bytes(digest)
            ),
            quota.work(),
        )?;
        if quota.work().is_some() {
            quota.adopt(key.capacity() as u64)?;
        }
        let fence = fence.clone_with_work(quota.work())?;
        if quota.work().is_some() {
            quota.adopt(fence.logical_owned_bytes()?)?;
        }
        coordinator
            .apply(
                IdempotencyKey::new(key)?,
                crate::scheduler::SchedulerEvent::ModelDispatchClaimed {
                    operation_id,
                    attempt_id: attempt,
                    step,
                    request_digest,
                    fence,
                    ceiling,
                },
                quota.work(),
            )
            .await?;
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn verify_model_history<'a>(
        &self,
        task: TaskId,
        operation: OperationId,
        records: impl Iterator<Item = &'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref() {
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
            &self.owner,
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
}
