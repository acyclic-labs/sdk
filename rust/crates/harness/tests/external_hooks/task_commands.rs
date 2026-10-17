//! Native requests execute stock admission/mail/cancellation, never plugin copies.
use super::{
    intake::deliver,
    session_runtime::{Fixture, VolumeBinding},
    *,
};
use acyclic_harness::{
    OperationId, Outcome, TaskId,
    distributed::{SchedulerPayloadStore, Worker},
    filesystem::{
        TASK_ADMIT_COMMAND_KIND, TOOL_TASK_COMMAND_KIND, TaskWorkerAttempt, TaskWorkerOutcome,
    },
    runtime::TaskStateProvider,
    task_volume::Component,
    workflow::WorkflowCommand,
};

#[tokio::test]
async fn all_hosts_use_stock_child_and_mail_commands_through_request_files() -> Result<()> {
    Box::pin(async {
        for host in [ExternalHost::Codex, ExternalHost::ClaudeCode, ExternalHost::Pi] {
            let fixture = Fixture::new(VolumeBinding::Recursive).await?;
            let attach = request(host, if host == ExternalHost::Pi {"session_start"} else {"SessionStart"},
                json!({"sessionId":"session","cwd":"/project"}))?;
            let folder = Component::new(attach.task_id().to_string()).map_err(storage)?;
            deliver(&fixture, &folder, &attach).await?;
            let unknown = call(&fixture, &folder, host, "unknown-definition", "acyclic_spawn", json!({
                "name":"unregistered.task","version":"1","input":{}
            })).await?;
            assert!(unknown.get("Failed").is_some(), "{unknown}");

            let written = call(&fixture, &folder, host, "mail-file", "write", json!({
                "path":"message.txt","text":"steer through a pinned file","media_type":"text/plain","display_name":"message.txt"
            })).await?;
            let payload = written["Succeeded"]["file"].clone();
            assert!(payload.is_object(), "{written}");
            for (index, name) in ["acyclic_send", "acyclic_steer"].into_iter().enumerate() {
                let sent = call(&fixture, &folder, host, name, name,
                    json!({"recipient":attach.task_id(),"payload":payload})).await?;
                assert!(sent.get("Succeeded").is_some(), "{sent}");
                let received = call(&fixture, &folder, host, &format!("receive-{index}"), "acyclic_receive",
                    json!({"after":index})).await?;
                assert_eq!(received["Succeeded"]["sequence"], index + 1);
                assert_eq!(received["Succeeded"]["payload"], payload);
            }

            let write = command(&fixture, TOOL_TASK_COMMAND_KIND, json!({
                "name":"write","revision":fixture.revision,"arguments":{
                    "path":"child.txt","text":"child effect","media_type":"text/plain","display_name":"child.txt"
                }
            })).await?;
            let spawn = native(host, "spawn", "acyclic_spawn", json!({
                "name":"acyclic.command_session","version":"1","input":{"command":write,"terminal":true}
            }))?;
            let spawned = deliver(&fixture, &folder, &spawn).await?;
            let child: TaskId = serde_json::from_value(spawned["Succeeded"].clone()).map_err(storage)?;
            assert_eq!(fixture.runtime.task_host().observe_admission(child).await?.parent, Some(attach.task_id()));
            run_child(&fixture, child, true).await?;
            assert_eq!(fixture.runtime.task_host().outcome(child).await?, Some(Outcome::Succeeded(Value::Null)));
            let inspected = call(&fixture, &folder, host, "inspect", "acyclic_inspect", json!({"task":child})).await?;
            assert_eq!(inspected["Succeeded"]["task"], child.to_string());
            assert_eq!(inspected["Succeeded"]["admission"]["parent"], attach.task_id().to_string());
            assert_eq!(inspected["Succeeded"]["outcome"], json!({"Succeeded":null}));
            let waited = call(&fixture, &folder, host, "wait", "acyclic_wait",
                json!({"child":child})).await?;
            assert_eq!(waited, json!({"Succeeded":null}));
            assert_eq!(spawn.send(&fixture.requests, &fixture.results, folder.clone()).await.map_err(storage)?, spawned,
                "retry reads the original child without a new request append");

            let grandchild = command(&fixture, TASK_ADMIT_COMMAND_KIND, json!({
                "name":"acyclic.command_session","version":"1","input":{"command":write,"terminal":true}
            })).await?;
            let pending = call(&fixture, &folder, host, "branch", "acyclic_spawn", json!({
                "name":"acyclic.command_session","version":"1","input":{"command":grandchild,"terminal":false}
            })).await?;
            let branch: TaskId = serde_json::from_value(pending["Succeeded"].clone()).map_err(storage)?;
            run_child(&fixture, branch, false).await?;
            let descendants = fixture.runtime.harness().children(branch, None, None, 4).await?;
            assert_eq!(descendants.entries.len(), 1);
            let leaf = descendants.entries[0].task_id;
            let cancelled = call(&fixture, &folder, host, "cancel", "acyclic_cancel", json!({"child":branch})).await?;
            assert!(cancelled.get("Succeeded").is_some(), "{cancelled}");
            assert_eq!(fixture.runtime.task_host().outcome(branch).await?, Some(Outcome::Cancelled));
            assert_eq!(fixture.runtime.task_host().outcome(leaf).await?, Some(Outcome::Cancelled));
            assert_eq!(fixture.runtime.task_host().outcome(child).await?, Some(Outcome::Succeeded(Value::Null)));
        }
        Ok(())
    }).await
}

fn native(host: ExternalHost, call: &str, name: &str, arguments: Value) -> Result<ExternalRequest> {
    request(
        host,
        if host == ExternalHost::Pi {
            "tool_call"
        } else {
            "PreToolUse"
        },
        json!({"sessionId":"session","toolCallId":call,"toolName":name,"input":arguments}),
    )
}

async fn call(
    fixture: &Fixture,
    folder: &Component,
    host: ExternalHost,
    id: &str,
    name: &str,
    input: Value,
) -> Result<Value> {
    deliver(fixture, folder, &native(host, id, name, input)?).await
}

async fn command(fixture: &Fixture, kind: &str, input: Value) -> Result<WorkflowCommand> {
    let operation_id = OperationId::new();
    let payload = fixture
        .publisher
        .stage_input(
            operation_id,
            &format!("recursive:{operation_id}"),
            &serde_json::to_vec(&input).map_err(storage)?,
            None,
        )?
        .await?;
    Ok(WorkflowCommand {
        operation_id,
        kind: kind.into(),
        payload,
    })
}

async fn run_child(fixture: &Fixture, task: TaskId, completed: bool) -> Result<()> {
    let worker = Worker {
        id: "child-worker".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let commands = fixture.runtime.commands();
    match Box::pin(
        fixture
            .runtime
            .run_operation(&worker, task, &commands, 16, None),
    )
    .await?
    {
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { .. })) if completed => {
            Ok(())
        }
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { .. })) if !completed => {
            Ok(())
        }
        Some(TaskWorkerAttempt::Unresolved { error, .. }) => Err(error),
        _ => Err(Error::Storage(
            "child worker did not reach its expected checkpoint".into(),
        )),
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
