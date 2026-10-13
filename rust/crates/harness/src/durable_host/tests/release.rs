use super::*;
use crate::executor::{ExecutionEvent, ExecutionJournal, ToolFailureKind};
use crate::workflow::{DurableWorkflowHost, WorkflowJournal};

fn work(
    budget: acyclic_fs::WorkBudget,
    token: &acyclic_fs::CancellationToken,
) -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(budget, token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

impl<P, A, O> Fixture<P, A, O>
where
    P: StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    async fn suspend(
        &self,
        byte: u8,
        workflow: &Arc<crate::filesystem::FilesystemWorkflowJournal<P, A, O>>,
        key: &str,
    ) -> Result<()> {
        let admission = self.owner(byte).await?.workflow_admission().clone();
        workflow.admit(admission.clone()).await?;
        DurableWorkflowHost::open(self.machines.clone(), admission.initial, workflow.clone())
            .await?
            .step(
                OperationId::from_bytes([70; 16]),
                IdempotencyKey::new(key)?,
                json!(0),
                |_| Ok(()),
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn b19_release_requires_suspended_workflow_and_settled_tool() -> Result<()> {
    let f = fixture::fixture().await?;
    f.running(1).await?;
    let attempt = OperationId::from_bytes([2; 16]);
    let journal = f.execution(f.owner(1).await?, attempt)?;
    let workflow = f.workflow(f.owner(1).await?)?;
    let token = acyclic_fs::CancellationToken::new();
    let work = work(
        acyclic_fs::performance::work_budget(100_000, 100_000_000),
        &token,
    )?;
    assert!(
        !journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    workflow
        .admit(f.owner(1).await?.workflow_admission().clone())
        .await?;
    assert!(
        !journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    journal
        .append(
            attempt,
            "started",
            &(ExecutionEvent::Started {
                request_digest: [9; 32],
            }),
            None,
        )?
        .await?;
    let invocation = crate::tool::ToolInvocation::for_model_call(
        attempt,
        0,
        "tool".into(),
        "test".into(),
        json!({}),
        None,
    )?;
    let invocation =
        crate::executor::stage_json(&journal, attempt, "invocation", &invocation, None).await?;
    journal
        .append(
            attempt,
            "tool",
            &(ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "tool".into(),
                invocation,
            }),
            None,
        )?
        .await?;
    f.suspend(1, &workflow, "suspend").await?;
    assert!(
        !journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    journal
        .append(
            attempt,
            "settle",
            &(ExecutionEvent::ToolFailed {
                step: 0,
                call_id: "tool".into(),
                reason: ToolFailureKind::ExecutorRejected,
            }),
            None,
        )?
        .await?;
    assert!(
        journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    assert!(!!matches!(
        f.host.recover_work(TaskId::from_bytes([1; 16])).await?,
        crate::distributed::WorkPull::Idle
    ));
    Ok(())
}

#[tokio::test]
async fn b20_public_release_rejects_foreign_bindings() -> Result<()> {
    let f = fixture::fixture().await?;
    f.running(1).await?;
    let attempt = OperationId::from_bytes([2; 16]);
    let journal = f.execution(f.owner(1).await?, attempt)?;
    let workflow = f.workflow(f.owner(1).await?)?;
    f.suspend(1, &workflow, "suspend").await?;
    journal
        .append(
            attempt,
            "started",
            &(ExecutionEvent::Started {
                request_digest: [9; 32],
            }),
            None,
        )?
        .await?;
    let token = acyclic_fs::CancellationToken::new();
    let work = work(
        acyclic_fs::performance::work_budget(100_000, 100_000_000),
        &token,
    )?;
    let replacement = f.replace(1).await?;
    let replaced_workflow = f.workflow(replacement)?;
    assert!(
        journal
            .release_if_quiescent_with_native_work(&work, &replaced_workflow)
            .await
            .is_err()
    );
    f.running(4).await?;
    let foreign = f.workflow(f.owner(4).await?)?;
    let current = f.execution(f.owner(1).await?, OperationId::from_bytes([5; 16]))?;
    assert!(
        current
            .release_if_quiescent_with_native_work(&work, &foreign)
            .await
            .is_err()
    );
    let other_volume = crate::filesystem::FilesystemWorkflowJournal::for_task(
        f.owner(1).await?,
        f.files.clone(),
        &f.destination,
        &f.issuer.verifier(&mut None)?,
        &f.signed,
        65_536,
        None,
    )?;
    assert!(
        current
            .release_if_quiescent_with_native_work(&work, &other_volume)
            .await
            .is_err()
    );
    assert!(!matches!(
        f.host.recover_work(TaskId::from_bytes([1; 16])).await?,
        crate::distributed::WorkPull::Idle
    ));
    Ok(())
}

#[tokio::test]
async fn b21_cancelled_or_zero_read_work_retains_reservation() -> Result<()> {
    let f = fixture::fixture().await?;
    f.running(1).await?;
    let attempt = OperationId::from_bytes([2; 16]);
    let journal = f.execution(f.owner(1).await?, attempt)?;
    let workflow = f.workflow(f.owner(1).await?)?;
    f.suspend(1, &workflow, "suspend").await?;
    journal
        .append(
            attempt,
            "started",
            &(ExecutionEvent::Started {
                request_digest: [9; 32],
            }),
            None,
        )?
        .await?;
    let budget = acyclic_fs::performance::work_budget(100_000, 100_000_000);
    for cancelled in [true, false] {
        let token = acyclic_fs::CancellationToken::new();
        let work = work(
            if cancelled {
                budget
            } else {
                acyclic_fs::WorkBudget {
                    backend_read_operations: 0,
                    ..budget
                }
            },
            &token,
        )?;
        if cancelled {
            token.cancel();
        }
        assert!(
            journal
                .release_if_quiescent_with_native_work(&work, &workflow)
                .await
                .is_err()
        );
        assert!(!matches!(
            f.host.recover_work(TaskId::from_bytes([1; 16])).await?,
            crate::distributed::WorkPull::Idle
        ));
    }
    Ok(())
}
