use super::{
    session_runtime::{Fixture, VolumeBinding},
    *,
};
use acyclic_harness::{
    OperationId, TaskId,
    distributed::Worker,
    filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
    runtime::TaskStateProvider,
    scheduler::ResourceRequest,
    task_volume::{Component, Query},
};
use futures::StreamExt as _;
use std::collections::BTreeMap;

#[tokio::test]
async fn native_request_files_admit_detach_and_reconnect_the_same_session() -> Result<()> {
    Box::pin(async {
        for host in [ExternalHost::Codex, ExternalHost::ClaudeCode, ExternalHost::Pi] {
            let fixture = Fixture::new(VolumeBinding::Separate).await?;
            let task = TaskId::from_bytes([75;16]);
            let folder = Component::new(task.to_string()).map_err(storage)?;
            let start = request(host, if host == ExternalHost::Pi {"session_start"} else {"SessionStart"},
                identity(host, json!({"cwd":"/project"})))?;
            let attached = deliver(&fixture,&folder,&start).await?;
            assert_eq!(attached["task"], task.to_string());
            let arguments = json!({"path":"session.txt","text":"written through native requests",
                "media_type":"text/plain","display_name":"session.txt"});
            let input = match host {
                ExternalHost::Pi=>json!({"toolCallId":"write-call","toolName":"write","input":{
                    "path":"session.txt","content":"written through native requests"}}),
                ExternalHost::ClaudeCode=>json!({"tool_use_id":"write-call","tool_name":"Write","tool_input":{
                    "file_path":"session.txt","content":"written through native requests"}}),
                ExternalHost::Codex=>json!({"tool_use_id":"write-call","tool_name":"write","tool_input":arguments}),
            };
            let write = request(host, if host == ExternalHost::Pi {"tool_call"} else {"PreToolUse"}, identity(host,input))?;
            let written = deliver(&fixture,&folder,&write).await?;
            let outcome: acyclic_harness::Outcome<acyclic_harness::tool::files::FileResult> =
                serde_json::from_value(written).map_err(|error| invalid(&error))?;
            let acyclic_harness::Outcome::Succeeded(output) = outcome else {
                return Err(Error::Storage("native request did not execute the write tool".into()));
            };
            assert_eq!(output.file.path(),"session.txt");
            let reader = fixture.runtime.harness().durable_context(task,OperationId::from_bytes(task.into_bytes()),None).await?;
            assert_eq!(&*reader.read_file(&output.file,None).await?,b"written through native requests");
            let completed_input=if host==ExternalHost::Pi {
                json!({"toolCallId":"write-call","isError":false,"content":[{"type":"text","text":"done"}]})
            } else {json!({"tool_use_id":"write-call","tool_response":{"ok":true}})};
            let completed=request(host,if host==ExternalHost::Pi {"tool_result"} else {"PostToolUse"},identity(host,completed_input))?;
            deliver(&fixture,&folder,&completed).await?;
            let foreign = request(host,if host==ExternalHost::Pi {"session_shutdown"} else {"SessionEnd"},
                if host==ExternalHost::Pi {json!({"sessionId":"other"})} else {json!({"session_id":"other"})})?;
            let file=publish(&fixture,&folder,&foreign).await?;
            let consumer=RequestConsumer::new(&fixture.runtime,&fixture.requests,&fixture.results,fixture.publisher.as_ref(),"/project");
            consumer.accept(&folder,&file).await?;
            execute(&fixture,task).await?;
            let rejected = foreign.send(&fixture.requests,&fixture.results,folder.clone()).await.map_err(storage)?;
            assert!(rejected.get("Failed").is_some(),"{rejected}");
            let stop = request(host,if host == ExternalHost::Pi {"session_shutdown"} else {"SessionEnd"},identity(host,json!({})))?;
            let detached = deliver(&fixture,&folder,&stop).await?;
            assert_eq!(detached["task"],task.to_string());
            assert_eq!(fixture.runtime.task_host().outcome(task).await?,None);
            assert_eq!(start.send(&fixture.requests, &fixture.results, folder.clone()).await.map_err(storage)?, attached);
            let next = request(host, if host == ExternalHost::Pi {"tool_call"} else {"PreToolUse"},
                identity(host, json!({"tool_use_id":"reconnected", "tool_name":"write", "tool_input":{
                    "path":"resumed.txt", "text":"after reconnect", "media_type":"text/plain", "display_name":"resumed.txt"
                }})))?;
            let answer = deliver(&fixture, &folder, &next).await?;
            let result = tool_result(answer)?;
            assert_eq!(result["isError"], false);
            assert_eq!(fixture.runtime.task_host().outcome(task).await?,None);
        }
        Ok(())
    }).await
}

#[tokio::test]
async fn intake_rejects_unbound_workspaces_and_unpublished_delivery_bytes() -> Result<()> {
    Box::pin(async {
        let fixture = Fixture::new(VolumeBinding::Separate).await?;
        let folder = Component::new(TaskId::from_bytes([76; 16]).to_string()).map_err(storage)?;
        let consumer = RequestConsumer::new(
            &fixture.runtime,
            &fixture.requests,
            &fixture.results,
            fixture.publisher.as_ref(),
            "/project",
        );
        let start = request(
            ExternalHost::Pi,
            "session_start",
            json!({"sessionId":"session","cwd":"/outside"}),
        )?;
        let file = publish(&fixture, &folder, &start).await?;
        assert!(matches!(
            consumer.accept(&folder, &file).await,
            Err(Error::Unauthorized(_))
        ));
        let unpublished = request(
            ExternalHost::Pi,
            "session_start",
            json!({"sessionId":"different","cwd":"/project"}),
        )?;
        assert!(matches!(
            consumer
                .accept(
                    &folder,
                    &unpublished.to_file(folder.clone()).map_err(storage)?
                )
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(
            fixture
                .runtime
                .harness()
                .reconcile_admission(OperationId::from_bytes([76; 16]))
                .await?
                .is_none()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn rejected_native_actions_settle_without_blocking_the_next_request() -> Result<()> {
    Box::pin(async {
        let fixture = Fixture::new(VolumeBinding::Separate).await?;
        let task = TaskId::from_bytes([81; 16]);
        let folder = Component::new(task.to_string()).map_err(storage)?;
        let host = ExternalHost::ClaudeCode;
        let start = request(
            host,
            "SessionStart",
            identity(host, json!({"cwd":"/project"})),
        )?;
        deliver(&fixture, &folder, &start).await?;
        for (event, input) in [
            (
                "PreToolUse",
                json!({"tool_use_id":"unknown", "tool_name":"unknown", "tool_input":{}}),
            ),
            (
                "PreToolUse",
                json!({"tool_use_id":"escape", "tool_name":"Write",
                "tool_input":{"file_path":"../escape", "content":"rejected"}}),
            ),
            (
                "PreToolUse",
                json!({"tool_use_id":"schema", "tool_name":"write",
                "tool_input":{"path":"incomplete.txt"}}),
            ),
            (
                "PreToolUse",
                json!({"tool_use_id":"task-schema", "tool_name":"acyclic_spawn",
                "tool_input":{"name":"missing-version","kind":"forged-executor"}}),
            ),
            (
                "PostToolUse",
                json!({"tool_use_id":"missing", "tool_response":{"ok":true}}),
            ),
            (
                "SubagentStart",
                json!({"agent_id":"child", "agent_type":"worker"}),
            ),
            ("SubagentStop", json!({"agent_id":"child"})),
        ] {
            let rejected = request(host, event, identity(host, input))?;
            let answer = deliver(&fixture, &folder, &rejected).await?;
            let outcome: acyclic_harness::Outcome<Value> =
                serde_json::from_value(answer.clone()).map_err(|error| invalid(&error))?;
            assert!(
                matches!(outcome, acyclic_harness::Outcome::Failed { .. }),
                "{event}: {answer}"
            );
            if event == "SubagentStart" {
                assert!(
                    answer["Failed"]["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("session")),
                    "the fork must reach the retained session limit: {answer}"
                );
                assert!(
                    fixture
                        .runtime
                        .harness()
                        .children(task, None, None, 1)
                        .await?
                        .entries
                        .is_empty()
                );
            }
            assert!(
                hook_answer(
                    host,
                    event,
                    &serde_json::to_vec(&answer).map_err(|error| invalid(&error))?
                )
                .is_err()
            );
        }
        let stop = request(host, "SessionEnd", identity(host, json!({})))?;
        deliver(&fixture, &folder, &stop).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_disconnected_hook_leaves_its_request_for_the_existing_worker() -> Result<()> {
    Box::pin(async {
        let fixture = Fixture::new(VolumeBinding::Separate).await?;
        let task = TaskId::from_bytes([82; 16]);
        let folder = Component::new(task.to_string()).map_err(storage)?;
        let start = request(
            ExternalHost::Pi,
            "session_start",
            json!({"sessionId":"session", "cwd":"/project"}),
        )?;
        deliver(&fixture, &folder, &start).await?;
        let write = request(
            ExternalHost::Pi,
            "tool_call",
            json!({
                "sessionId":"session", "toolCallId":"disconnected", "toolName":"write",
                "input":{"path":"detached.txt", "content":"retained after disconnect"}
            }),
        )?;
        let mut client = Box::pin(write.send(&fixture.requests, &fixture.results, folder.clone()));
        assert!(futures::poll!(&mut client).is_pending());
        drop(client);
        let file = write.to_file(folder.clone()).map_err(storage)?;
        let accepted = fixture
            .requests
            .snapshot(&Query::File(file.key.clone()), 1)
            .await
            .map_err(storage)?;
        assert_eq!(accepted.files, vec![file.clone()]);
        RequestConsumer::new(
            &fixture.runtime,
            &fixture.requests,
            &fixture.results,
            fixture.publisher.as_ref(),
            "/project",
        )
        .accept(&folder, &file)
        .await?;
        execute(&fixture, task).await?;
        let answer = write
            .send(&fixture.requests, &fixture.results, folder)
            .await
            .map_err(storage)?;
        let outcome: acyclic_harness::Outcome<acyclic_harness::tool::files::FileResult> =
            serde_json::from_value(answer).map_err(|error| invalid(&error))?;
        let acyclic_harness::Outcome::Succeeded(written) = outcome else {
            return Err(Error::Storage(
                "disconnected request did not execute".into(),
            ));
        };
        let context = fixture
            .runtime
            .harness()
            .durable_context(task, OperationId::from_bytes(task.into_bytes()), None)
            .await?;
        assert_eq!(
            &*context.read_file(&written.file, None).await?,
            b"retained after disconnect"
        );
        let after = fixture
            .requests
            .snapshot(&Query::File(file.key), 1)
            .await
            .map_err(storage)?;
        assert_eq!(accepted.generation, after.generation);
        Ok(())
    })
    .await
}

pub(super) async fn publish(
    fixture: &Fixture,
    folder: &Component,
    request: &ExternalRequest,
) -> Result<acyclic_harness::task_volume::FileValue> {
    let file = request.to_file(folder.clone()).map_err(storage)?;
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
    Ok(file)
}

pub(super) async fn deliver(
    fixture: &Fixture,
    folder: &Component,
    request: &ExternalRequest,
) -> Result<Value> {
    let file = request.to_file(folder.clone()).map_err(storage)?;
    let mut incoming = fixture
        .requests
        .observe(Query::File(file.key.clone()), None, 1)
        .await
        .map_err(storage)?;
    let consumer = RequestConsumer::new(
        &fixture.runtime,
        &fixture.requests,
        &fixture.results,
        fixture.publisher.as_ref(),
        "/project",
    );
    let client = async {
        request
            .send(&fixture.requests, &fixture.results, folder.clone())
            .await
            .map_err(storage)
    };
    let actor = async {
        let delivered = incoming
            .changes
            .next()
            .await
            .ok_or_else(|| Error::Storage("request follow ended".into()))?
            .map_err(storage)?
            .file
            .ok_or_else(|| Error::Storage("follow omitted the request".into()))?;
        let admission = consumer.accept(folder, &delivered);
        assert!(std::mem::size_of_val(&admission) < 16 * 1024);
        admission.await?;
        consumer.accept(folder, &delivered).await?;
        let task = TaskId::from_bytes(
            uuid::Uuid::parse_str(folder.as_str())
                .map_err(|error| Error::Invalid(error.to_string()))?
                .into_bytes(),
        );
        execute(fixture, task).await?;
        consumer.accept(folder, &delivered).await?;
        Ok::<_, Error>(())
    };
    let (answer, ()) = tokio::try_join!(client, actor)?;
    Ok(answer)
}

pub(super) async fn execute(fixture: &Fixture, task: TaskId) -> Result<()> {
    let worker = Worker {
        id: "native-session".into(),
        available: ResourceRequest::default(),
        labels: BTreeMap::new(),
    };
    let commands = RequestCommands::new(
        &fixture.runtime,
        &fixture.requests,
        &fixture.results,
        fixture.publisher.as_ref(),
    );
    match Box::pin(
        fixture
            .runtime
            .run_operation(&worker, task, &commands, 8, None),
    )
    .await?
    {
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended {
            task: suspended, ..
        })) => assert_eq!(suspended, task),
        Some(TaskWorkerAttempt::Unresolved { error, .. }) => return Err(error),
        _ => {
            return Err(Error::Storage(
                "native request left an unresolved worker attempt".into(),
            ));
        }
    }
    Ok(())
}

fn identity(host: ExternalHost, mut input: Value) -> Value {
    input[if host == ExternalHost::Pi {
        "sessionId"
    } else {
        "session_id"
    }] = json!("session");
    input
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[tokio::test]
async fn retained_abort_prevents_dispatch_in_an_explicitly_bound_task() -> Result<()> {
    Box::pin(async {
        let fixture = Fixture::new(VolumeBinding::Separate).await?;
        let task = TaskId::from_bytes([92; 16]);
        let folder = Component::new(task.to_string()).map_err(storage)?;
        let start = request(
            ExternalHost::Pi,
            "session_start",
            json!({"sessionId":"session","cwd":"/project"}),
        )?;
        assert_ne!(
            start.task_id(),
            task,
            "exercise a platform-selected task binding"
        );
        deliver(&fixture, &folder, &start).await?;
        let abort = request(
            ExternalHost::Pi,
            "tool_cancel",
            json!({"sessionId":"session","toolCallId":"cancelled"}),
        )?;
        let acknowledgement = deliver(&fixture, &folder, &abort).await?;
        assert_eq!(
            acknowledgement["operation_id"],
            abort.operation_id.to_string()
        );
        let write = request(
            ExternalHost::Pi,
            "tool_call",
            json!({"sessionId":"session","toolCallId":"cancelled",
            "toolName":"write","input":{"path":"cancelled.txt","content":"must not publish"}}),
        )?;
        let answer = deliver(&fixture, &folder, &write).await?;
        assert!(answer.get("Failed").is_some(), "{answer}");
        let grant = acyclic_harness::conversation::ContentGrant::verify_directory_read(
            &fixture.verifier,
            &fixture.signed,
            &fixture.volume,
            "",
            &mut None,
        )?;
        assert!(matches!(
            fixture
                .files
                .read_private_path(&fixture.volume, &grant, "cancelled.txt", None, 1024, None)
                .await,
            Err(Error::NotFound(_))
        ));
        let next = request(
            ExternalHost::Pi,
            "tool_call",
            json!({"sessionId":"session","toolCallId":"after-cancel",
            "toolName":"write","input":{"path":"after.txt","content":"still admitted"}}),
        )?;
        let answer = deliver(&fixture, &folder, &next).await?;
        assert!(answer.get("Succeeded").is_some(), "{answer}");
        assert_eq!(
            fixture
                .files
                .read_private_path(&fixture.volume, &grant, "after.txt", None, 1024, None)
                .await?
                .1
                .as_ref(),
            b"still admitted"
        );
        Ok(())
    })
    .await
}
