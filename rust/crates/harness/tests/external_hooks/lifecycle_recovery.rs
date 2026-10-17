use super::{
    session_runtime::{Fixture, VolumeBinding},
    *,
};
use acyclic_harness::{
    TaskId,
    distributed::Worker,
    filesystem::{TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt, TaskWorkerOutcome},
    runtime::TaskContext,
    scheduler::{LeaseFence, ResourceRequest},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile},
    workflow::WorkflowCommand,
};
use acyclic_stream::BoxProviderFuture;
use std::collections::BTreeMap;

#[tokio::test]
async fn retained_lifecycle_intent_can_settle_during_reconciliation() -> Result<()> {
    Box::pin(async {
        let fixture = Fixture::new(VolumeBinding::Separate).await?;
        let task = TaskId::from_bytes([79; 16]);
        let folder = Component::new(task.to_string()).map_err(storage)?;
        let native = request(
            ExternalHost::Pi,
            "session_start",
            json!({"sessionId":"session","cwd":"/project"}),
        )?;
        let file = native.to_file(folder.clone()).map_err(storage)?;
        let snapshot = fixture
            .requests
            .snapshot(&Query::File(file.key.clone()), 1)
            .await
            .map_err(storage)?;
        fixture
            .requests
            .append(snapshot.generation, &file)
            .await
            .map_err(storage)?;
        RequestConsumer::new(
            &fixture.runtime,
            &fixture.requests,
            &fixture.results,
            fixture.publisher.as_ref(),
            "/project",
        )
        .accept(&folder, &file)
        .await?;
        let worker = Worker {
            id: "lifecycle-recovery".into(),
            available: ResourceRequest::default(),
            labels: BTreeMap::new(),
        };
        let interrupted = Interrupted { fixture: &fixture };
        let lease = match Box::pin(fixture.runtime.run_operation(
            &worker,
            task,
            &interrupted,
            8,
            None,
        ))
        .await?
        {
            Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease })) => lease,
            Some(TaskWorkerAttempt::Unresolved { error, .. }) => return Err(error),
            _ => {
                return Err(Error::Storage(
                    "interrupted command did not retain its lease".into(),
                ));
            }
        };
        let reservation = lease.reservation.id.clone();
        let commands = RequestCommands::new(
            &fixture.runtime,
            &fixture.requests,
            &fixture.results,
            fixture.publisher.as_ref(),
        );
        match Box::pin(
            fixture
                .runtime
                .reconcile_task(lease, &commands, 1, 1000, None),
        )
        .await
        {
            TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => {
                assert_eq!(lease.reservation.id, reservation);
                assert_eq!(
                    lease.operation.operation_id,
                    acyclic_harness::OperationId::from_bytes(task.into_bytes())
                );
            }
            TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
            _ => {
                return Err(Error::Storage(
                    "reconciliation advanced the task instead of retaining its lease".into(),
                ));
            }
        }
        let snapshot = fixture
            .results
            .snapshot(&Query::File(native.result_key(folder).map_err(storage)?), 1)
            .await
            .map_err(storage)?;
        assert_eq!(snapshot.files.len(), 1);
        let answer: Value =
            serde_json::from_slice(&snapshot.files[0].bytes).map_err(|error| invalid(&error))?;
        assert_eq!(answer["task"], task.to_string());
        Ok(())
    })
    .await
}

// Seed the retained started record and interrupt before the result.
// The real worker retains and reconciles that command.
struct Interrupted<'a> {
    fixture: &'a Fixture,
}

impl TaskCommandHost for Interrupted<'_> {
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        _payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let task = context
                .durable_task_id()
                .ok_or_else(|| Error::Unauthorized("no task".into()))?;
            let owner = self
                .fixture
                .runtime
                .task_host()
                .journal_owner(task, &fence, None)
                .await?;
            owner.verify(false, &mut None).await?;
            let started = FileValue::new(
                FileKey::Task {
                    task: Component::new(task.to_string()).map_err(storage)?,
                    file: TaskFile::RequestStarted(
                        Component::new(command.operation_id.to_string()).map_err(storage)?,
                    ),
                },
                serde_json::to_vec(&command).map_err(|error| invalid(&error))?,
            );
            let snapshot = self
                .fixture
                .results
                .snapshot(&Query::File(started.key.clone()), 1)
                .await
                .map_err(storage)?;
            self.fixture
                .results
                .append(snapshot.generation, &started)
                .await
                .map_err(storage)?;
            Ok(TaskCommandProgress::Indeterminate)
        })
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
