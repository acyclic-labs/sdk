//! Host-owned native children share the fenced task admission and join path.
use super::{
    intake::{deliver, execute, publish},
    session_runtime::{Fixture, VolumeBinding},
    *,
};
use acyclic_harness::{
    Outcome, TaskId,
    distributed::Worker,
    filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
    runtime::TaskStateProvider,
    task_volume::Component,
};

#[tokio::test]
async fn native_child_hooks_admit_attach_complete_and_join_for_every_host() -> Result<()> {
    Box::pin(async {
        for host in [
            ExternalHost::Codex,
            ExternalHost::ClaudeCode,
            ExternalHost::Pi,
        ] {
            let fixture = Fixture::new(VolumeBinding::Recursive).await?;
            let pi = host == ExternalHost::Pi;
            let parent = request(
                host,
                if pi { "session_start" } else { "SessionStart" },
                json!({"sessionId":"session","cwd":"/project"}),
            )?;
            let folder = Component::new(parent.task_id().to_string()).map_err(storage)?;
            deliver(&fixture, &folder, &parent).await?;
            let fork = request(
                host,
                if pi {
                    "subagent_start"
                } else {
                    "SubagentStart"
                },
                json!({"sessionId":"session","agentId":"child","agentType":"worker"}),
            )?;
            let admitted = deliver(&fixture, &folder, &fork).await?;
            let child: TaskId =
                serde_json::from_value(admitted["Succeeded"].clone()).map_err(storage)?;
            assert_eq!(
                fixture
                    .runtime
                    .task_host()
                    .observe_admission(child)
                    .await?
                    .parent,
                Some(parent.task_id())
            );
            let child_folder = Component::new(child.to_string()).map_err(storage)?;
            let attached = request(
                host,
                if pi { "session_start" } else { "SessionStart" },
                json!({"sessionId":"session","agentId":"child","cwd":"/project"}),
            )?;
            assert_eq!(attached.task_id(), child);
            let consumer = RequestConsumer::new(
                &fixture.runtime,
                &fixture.requests,
                &fixture.results,
                fixture.publisher.as_ref(),
                "/project",
            );
            // Claim before SessionStart to exercise the fenced initial input.
            execute(&fixture, child).await?;
            let file = attached.to_file(child_folder.clone()).map_err(storage)?;
            consumer.accept(&child_folder, &file).await?;
            let mut forged = attached.clone();
            forged.action = ExternalAction::Attach {
                workspace: "/another-project".into(),
            };
            assert!(
                forged
                    .send(&fixture.requests, &fixture.results, child_folder.clone())
                    .await
                    .is_err(),
                "a precomputed acknowledgement cannot admit changed native input"
            );
            assert_eq!(
                attached
                    .send(&fixture.requests, &fixture.results, child_folder.clone())
                    .await
                    .map_err(storage)?["task"],
                child.to_string()
            );
            assert_eq!(
                fork.send(&fixture.requests, &fixture.results, folder.clone())
                    .await
                    .map_err(storage)?,
                admitted
            );

            let join = request(
                host,
                if pi { "subagent_stop" } else { "SubagentStop" },
                json!({"sessionId":"session","agentId":"child"}),
            )?;
            let file = publish(&fixture, &folder, &join).await?;
            consumer.accept(&folder, &file).await?;
            execute(&fixture, parent.task_id()).await?;
            assert!(
                fixture
                    .results
                    .snapshot(
                        &acyclic_harness::task_volume::Query::File(
                            join.result_key(folder.clone()).map_err(storage)?
                        ),
                        1
                    )
                    .await
                    .map_err(storage)?
                    .files
                    .is_empty(),
                "join cannot complete while the child is running"
            );

            let other = request(
                host,
                if pi { "session_start" } else { "SessionStart" },
                json!({"sessionId":"session","agentId":"other","cwd":"/project"}),
            )?;
            deliver(
                &fixture,
                &Component::new(other.task_id().to_string()).map_err(storage)?,
                &other,
            )
            .await?;
            let wrong_parent = request(
                host,
                if pi { "subagent_stop" } else { "SubagentStop" },
                json!({"sessionId":"session","agentId":"child","parentAgentId":"other"}),
            )?;
            assert!(matches!(
                RequestConsumer::child_completion(
                    &fixture.runtime,
                    &fixture.results,
                    other.task_id(),
                    &wrong_parent
                )
                .await,
                Err(Error::Unauthorized(_))
            ));
            // SubagentStop supplies the terminal child request; the native
            // executor does not need to emit an additional SessionEnd.
            let completing = RequestConsumer::child_completion(
                &fixture.runtime,
                &fixture.results,
                parent.task_id(),
                &join,
            );
            let size = std::mem::size_of_val(&completing);
            eprintln!("native child completion future: {size} bytes");
            assert!(
                size < 16 * 1024,
                "native child completion future grew to {size} bytes"
            );
            let stop = completing.await?;
            let file = publish(&fixture, &child_folder, &stop).await?;
            consumer.accept(&child_folder, &file).await?;
            let commands = RequestCommands::new(
                &fixture.runtime,
                &fixture.requests,
                &fixture.results,
                fixture.publisher.as_ref(),
            );
            let worker = Worker {
                id: "native-child".into(),
                available: Default::default(),
                labels: Default::default(),
            };
            match Box::pin(
                fixture
                    .runtime
                    .run_operation(&worker, child, &commands, 16, None),
            )
            .await?
            {
                Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task })) => {
                    assert_eq!(task, child)
                }
                Some(TaskWorkerAttempt::Unresolved { error, .. }) => return Err(error),
                _ => return Err(Error::Storage("native child did not complete".into())),
            }
            assert_eq!(
                fixture.runtime.task_host().outcome(child).await?,
                Some(Outcome::Succeeded(Value::Null))
            );
            execute(&fixture, parent.task_id()).await?;
            assert_eq!(
                join.send(&fixture.requests, &fixture.results, folder.clone())
                    .await
                    .map_err(storage)?,
                json!({"Succeeded":null})
            );
            assert_eq!(
                fixture
                    .runtime
                    .task_host()
                    .outcome(parent.task_id())
                    .await?,
                None,
                "joining a child keeps its parent attached"
            );
        }
        Ok(())
    })
    .await
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[tokio::test]
async fn stop_cannot_swap_the_sessions_of_two_admitted_child_labels() -> Result<()> {
    Box::pin(async {
        let fixture = Fixture::new(VolumeBinding::Recursive).await?;
        let event = |event, input| request(ExternalHost::Pi, event, input);
        let parent = event(
            "session_start",
            json!({"sessionId":"parent","cwd":"/project"}),
        )?;
        let folder = Component::new(parent.task_id().to_string()).map_err(storage)?;
        deliver(&fixture, &folder, &parent).await?;
        let identity = |label, session| {
            json!({"sessionId":"parent","agentId":label,
            "childSession":{"host":"pi","session_id":session,"caller":null}})
        };
        let mut children = Vec::new();
        for (label, session) in [("a", "native-a"), ("b", "native-b")] {
            let fork = event("subagent_start", identity(label, session))?;
            let answer = deliver(&fixture, &folder, &fork).await?;
            let child: TaskId =
                serde_json::from_value(answer["Succeeded"].clone()).map_err(storage)?;
            execute(&fixture, child).await?;
            children.push(child);
        }
        for (label, session) in [("a", "native-b"), ("b", "native-a")] {
            let stop = event("subagent_stop", identity(label, session))?;
            assert!(matches!(
                RequestConsumer::child_completion(
                    &fixture.runtime,
                    &fixture.results,
                    parent.task_id(),
                    &stop,
                )
                .await,
                Err(Error::Unauthorized(_))
            ));
        }
        for (index, session) in ["native-a", "native-b"].into_iter().enumerate() {
            assert_eq!(
                fixture.runtime.task_host().outcome(children[index]).await?,
                None
            );
            let write = event(
                "tool_call",
                json!({"sessionId":session,"toolCallId":"after-swapped-stop",
                "toolName":"write","input":{"path":format!("{session}.txt"),"content":session}}),
            )?;
            let answer = deliver(
                &fixture,
                &Component::new(children[index].to_string()).map_err(storage)?,
                &write,
            )
            .await?;
            assert_eq!(tool_result(answer)?["isError"], false);
        }
        for (index, (label, session)) in [("a", "native-a"), ("b", "native-b")]
            .into_iter()
            .enumerate()
        {
            let stop = event("subagent_stop", identity(label, session))?;
            let completion = RequestConsumer::child_completion(
                &fixture.runtime,
                &fixture.results,
                parent.task_id(),
                &stop,
            )
            .await?;
            assert_eq!(completion.task_id(), children[index]);
        }
        Ok(())
    })
    .await
}
