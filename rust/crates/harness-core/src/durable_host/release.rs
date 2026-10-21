//! Atomic release of settled execution and suspended workflow journals.
use super::*;

impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) async fn release_quiescent_execution(
        &self,
        execution: OperationId,
        tail: u64,
        pending: &crate::filesystem::WorkflowReleaseWitness<'_>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        let (binding, workflow) = pending.condition();
        if binding != self.task_binding() {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "pending workflow belongs to another task or fence",
            ));
        }
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            &self.host.owner,
            &self.host.owner_scope,
            &self.host.verifier,
            self.operation_id(),
            work,
        )?;
        crate::scheduler::require_execution_owner(operation, &self.fence, true, work)?;
        if operation.cancellation_requested {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Conflict,
                "read-only close cannot settle task cancellation",
            ));
        }
        if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            return Ok(false);
        }
        let key = worker_key(
            "release",
            self.operation_id(),
            &self.fence,
            operation.revision,
            work,
        )?;
        let fence = self.fence.clone_with_work(work)?;
        coordinator
            .apply_if_execution_quiescent(
                key,
                crate::scheduler::SchedulerEvent::LeaseReleased {
                    operation_id: self.operation_id(),
                    fence,
                },
                execution,
                tail,
                Some(workflow),
                work,
            )
            .await?;
        Ok(true)
    }
}
