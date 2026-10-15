//! Read-only Pi workflow replay through the SDK host.
use super::*;
use acyclic_harness::workflow::{DurableWorkflowHost, WorkflowJournal};

impl PiTaskRuntime {
    pub(crate) async fn workflow(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(DurableWorkflowHost, acyclic_fs::NativeAllocation)> {
        let mut registry = MachineRegistry::default();
        let registry_custody =
            registry.register_with_native_work(PiTaskMachine::new(work)?, work)?;
        let journal = self
            .runtime
            .readonly_workflow_journal_with_native_work(task, work)
            .await?;
        let io = PreparationIo::from_native_work(work)?;
        let (admission, _admission_custody) = journal.admission(Some(&io))?.await?;
        let initial = admission
            .ok_or(acyclic_harness::Error::Indeterminate(
                OperationId::from_bytes(task.into_bytes()),
            ))?
            .initial;
        let workflow =
            DurableWorkflowHost::open_with_native_work(registry, initial, journal, work).await?;
        Ok((workflow, registry_custody))
    }
}
