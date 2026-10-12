//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Authenticates retained model claims without requiring an execution lease.
    /// Passive readers gain no publication or provider-dispatch authority.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn verify_model_history(
        &self,
        task: TaskId,
        operation: OperationId,
        records: &[crate::executor::ExecutionRecord],
    ) -> Result<()> {
        if !records.iter().any(|record| {
            matches!(
                record.event,
                crate::executor::ExecutionEvent::ModelStarted { .. }
            )
        }) {
            return Ok(());
        }
        let task_operation = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                task_operation,
                &mut None,
            )?
            .clone();
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
                    purpose.attempt_operation(operation),
                    *step,
                    request_digest,
                )?;
            }
        }
        Ok(())
    }
}
