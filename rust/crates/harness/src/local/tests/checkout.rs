//! Stock default commands execute against and publish the same checkout.
use super::*;

fn call(index: usize, name: &str, command: &str) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall {
            call_id: format!("checkout-{index}"),
            name: name.into(),
            arguments: json!({"command":command}),
        }),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

const COMMANDS: [(&str, &str); 8] = [
    (
        "bash",
        "printf original > note.txt; printf remove > doomed.txt",
    ),
    ("fs", "add note.txt"),
    ("fs", "commit -m initial"),
    (
        "bash",
        "printf changed > note.txt; mkdir nested; mv doomed.txt nested/moved.txt",
    ),
    ("fs", "diff"),
    ("fs", "diff"),
    (
        "bash",
        "rm nested/moved.txt; printf final > note.txt; printf visible",
    ),
    ("fs", "status"),
];

#[tokio::test]
async fn default_bash_and_fs_share_the_published_checkout() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
    let provider = scripted(
        COMMANDS
            .iter()
            .enumerate()
            .map(|(index, (name, command))| call(index, name, command))
            .chain([ScriptedTurn::Text("commands completed".into())]),
    );
    let harness = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model()?, provider.clone()),
    ))
    .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) = harness
        .submit(OperationId::new(), "edit and inspect the checkout")
        .await?
    else {
        return Err(Error::Conflict("stock checkout was not admitted".into()));
    };
    let commands = harness.commands()?;
    harness
        .advance(task, &super::checked_commands::Checked(&commands))
        .await?;
    let output = harness.resume(task).await;
    if let Err(error) = &output {
        let requests = provider.requests();
        panic!(
            "{error:?}; {} model requests; last message: {:?}",
            requests.len(),
            requests.last().and_then(|request| request.messages.last())
        );
    }
    assert_eq!(output?.text, "commands completed");
    let requests = provider.requests();
    assert_eq!(requests.len(), COMMANDS.len() + 1);
    let mounts = harness.workspace().unwrap().mounts.context_line();
    for request in &requests {
        assert_eq!(
            request
                .messages
                .iter()
                .filter(|message| message.role == crate::model::ModelRole::System
                    && message.content == crate::model::ModelContent::Text(mounts.clone()))
                .count(),
            1,
            "each model step sees the same mount table used by bash"
        );
    }
    let results: std::collections::BTreeMap<_, _> = requests
        .iter()
        .flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id,
                name,
                content,
            } => Some((call_id.as_str(), (name, content))),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), COMMANDS.len());
    for (name, result) in results.values() {
        let encoded = serde_json::to_string(result).map_err(super::super::storage)?;
        assert!(
            !encoded.contains("workspace tools are not bound"),
            "{name}: {encoded}"
        );
        assert!(!encoded.contains("\"error\""), "{name}: {encoded}");
    }
    assert!(
        serde_json::to_string(
            results[format!(
                "checkout-{}",
                COMMANDS
                    .iter()
                    .position(|(_, command)| command.ends_with("printf visible"))
                    .unwrap()
            )
            .as_str()]
            .1
        )
        .map_err(super::super::storage)?
        .contains("visible")
    );
    assert_eq!(
        std::fs::read(directory.path().join("note.txt")).map_err(super::super::storage)?,
        b"final"
    );
    let output_files = harness
        .task_files()
        .snapshot(&crate::task_volume::Query::Tasks, 32)
        .await?;
    let steps: std::collections::BTreeSet<_> = output_files
        .files
        .iter()
        .filter_map(|file| match &file.key {
            crate::task_volume::FileKey::Task {
                file:
                    crate::task_volume::TaskFile::Tool {
                        turn,
                        file: crate::task_volume::ToolFile::Request,
                        ..
                    },
                ..
            } => Some(*turn),
            _ => None,
        })
        .collect();
    assert_eq!(steps, [0, 3, 6].into_iter().collect());
    let view = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout view".into()))?
        .view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let head = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value;
    let bytes = head
        .read(
            "/note.txt",
            1024,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(bytes.as_ref(), b"final");
    assert!(!directory.path().join("nested/moved.txt").exists());
    Ok(())
}

#[tokio::test]
async fn checkout_commands_require_their_admitted_context_and_never_relaunch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
    std::fs::write(directory.path().join("unopened"), b"uncaptured")
        .map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model()?, provider.clone()),
    ))
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) =
        harness.submit(operation, "command authority").await?
    else {
        return Err(Error::Conflict("stock admission rejected".into()));
    };
    let worker = crate::distributed::Worker {
        id: "checkout-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        return Err(Error::Conflict("task was not claimed".into()));
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    let task_context = Arc::new(
        harness
            .runtime
            .task_context(task, crate::scheduler::LeaseFence::from(&lease.reservation))
            .await?,
    );
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("workspace".into()))?;
    assert!(matches!(
        workspace.executor(
            harness.scratch(),
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            Arc::clone(harness.task_files())
        ),
        Err(Error::Unauthorized(_))
    ));
    let cancellation = acyclic_fs::CancellationToken::new();
    let head = workspace
        .view()
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value
        .id();
    let bash = harness
        .tools()
        .get("bash", None, None)?
        .ok_or_else(|| Error::NotFound("bash".into()))?;
    for cwd in ["..", "src/../outside", "/absolute"] {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "invalid-cwd".into(),
            name: "bash".into(),
            arguments: json!({"command":"printf unwanted > result", "cwd":cwd}),
        };
        let context = ToolContext::new(
            Arc::clone(&task_context),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        assert!(matches!(
            bash.executor.execute(invocation, Some(context)).await,
            Err(Error::Invalid(_))
        ));
    }
    assert_eq!(
        workspace
            .view()
            .workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value
            .id(),
        head
    );
    assert!(
        harness
            .task_files()
            .snapshot(&crate::task_volume::Query::Tasks, 0)
            .await?
            .files
            .is_empty()
    );
    // Property: arbitrary call identities are authorized exactly when both
    // identities and every required grant survive scope attenuation.
    {
        use proptest::prelude::*;
        let read = harness
            .project()
            .capability(VolumeOperation::Read, &mut None)?;
        let write = harness
            .project()
            .capability(VolumeOperation::Write, &mut None)?;
        let tool = harness
            .tools()
            .get("bash", None, None)?
            .ok_or_else(|| Error::NotFound("bash".into()))?;
        proptest::test_runner::TestRunner::new(ProptestConfig {
            source_file: Some(file!()),
            ..ProptestConfig::with_cases(64)
        })
        .run(
            &(0u8..32, any::<[u8; 16]>(), "[a-z]{1,32}"),
            |(mask, bytes, call)| {
                let grants = [read.as_str(), write.as_str(), "tool:call:bash"]
                    .into_iter()
                    .enumerate()
                    .filter(|(index, _)| mask & (1 << index) != 0)
                    .map(|(_, grant)| grant.to_owned());
                let task = task_context
                    .scoped(
                        crate::Capabilities::new(grants),
                        task_context.scope().limits(),
                    )
                    .unwrap();
                let context =
                    ToolContext::new(Arc::new(task), OperationId::from_bytes(bytes), &call, None)
                        .unwrap();
                let mut invocation_bytes = bytes;
                if mask & 8 == 0 {
                    invocation_bytes[0] ^= 1;
                }
                let invocation = ToolInvocation {
                    operation_id: OperationId::from_bytes(invocation_bytes),
                    call_id: if mask & 16 != 0 {
                        call
                    } else {
                        format!("{call}-other")
                    },
                    name: "bash".into(),
                    arguments: json!({"command":"true"}),
                };
                prop_assert_eq!(
                    tool.executor
                        .authorize(None, Some(&context), &invocation, None)
                        .is_ok(),
                    mask == 31
                );
                Ok(())
            },
        )
        .map_err(super::super::storage)?;
    }
    for (name, command) in COMMANDS {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "direct".into(),
            name: name.into(),
            arguments: json!({"command":command}),
        };
        let tool = harness
            .tools()
            .get(name, None, None)?
            .ok_or_else(|| Error::NotFound(name.into()))?;
        assert!(matches!(
            tool.executor.execute(invocation.clone(), None).await,
            Err(Error::Unauthorized(_))
        ));
        let narrowed = task_context.scoped(
            crate::Capabilities::new([
                harness
                    .project()
                    .capability(VolumeOperation::Read, &mut None)?,
                harness
                    .project()
                    .capability(VolumeOperation::Write, &mut None)?,
            ]),
            task_context.scope().limits(),
        )?;
        let denied = ToolContext::new(
            Arc::new(narrowed),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        assert!(matches!(
            tool.executor
                .execute(invocation.clone(), Some(denied))
                .await,
            Err(Error::Unauthorized(_))
        ));
        let context = ToolContext::new(
            task_context.clone(),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        assert!(
            tool.executor
                .reconcile(invocation.clone(), Some(context.clone()))
                .await?
                .is_none()
        );
        let result = tool.executor.execute(invocation, Some(context)).await;
        assert!(result.is_ok(), "{name} {command}: {result:?}");
        let result = result?;
        crate::executor::validate_json_schema_value(
            &tool.definition.output_schema,
            &result.value,
            "checkout tool output",
            &mut None,
        )?;
        if command == "add note.txt" {
            assert_eq!(result.value, json!({"result":"NoOp"}));
        }
        if command.ends_with("printf visible") {
            assert_eq!(result.value["stdout"], "visible");
        }
    }
    assert_eq!(
        std::fs::read(directory.path().join("note.txt")).map_err(super::super::storage)?,
        b"final"
    );
    assert_eq!(provider.requests().len(), 0);
    // A retained scope does not retain execution ownership after settlement.
    harness
        .runtime
        .task_host()
        .settle_task(
            task,
            &crate::scheduler::LeaseFence::from(&lease.reservation),
            crate::Outcome::Failed {
                message: "retired owner".into(),
            },
            &mut None,
        )
        .await?;
    for (name, command) in [("fs", "clean -f"), ("bash", "rm unopened")] {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "retired-owner".into(),
            name: name.into(),
            arguments: json!({"command":command}),
        };
        let context = ToolContext::new(
            Arc::clone(&task_context),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        let tool = harness
            .tools()
            .get(name, None, None)?
            .ok_or_else(|| Error::NotFound(name.into()))?;
        assert!(
            tool.executor
                .execute(invocation, Some(context))
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read(directory.path().join("unopened")).map_err(super::super::storage)?,
            b"uncaptured"
        );
    }
    Ok(())
}

fn input_call(arguments: Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall {
            call_id: "bash_input_1".into(),
            name: "bash".into(),
            arguments,
        }),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

#[tokio::test]
async fn bash_cwd_exit_code_and_timeout_publish_their_writes() -> Result<()> {
    let cases = [
        (
            json!({"command":"printf captured > result; printf stdout; printf stderr >&2; exit 7", "cwd":"src", "timeout":5}),
            Some(7),
            false,
        ),
        (
            json!({"command":"printf captured > result; while :; do :; done", "cwd":"src", "timeout":1}),
            None,
            true,
        ),
    ];
    let outcomes =
        futures::future::join_all(cases.into_iter().map(|(input, code, timeout)| async move {
            let directory = tempfile::tempdir().map_err(super::super::storage)?;
            std::fs::create_dir(directory.path().join("src")).map_err(super::super::storage)?;
            let provider = scripted([input_call(input), ScriptedTurn::Text("done".into())]);
            let harness = LocalHarness::open(LocalHarnessConfig::new(
                directory.path(),
                (model()?, provider.clone()),
            ))
            .await?;
            harness.run("execute bash").await?;
            assert_eq!(
                std::fs::read(directory.path().join("src/result"))
                    .map_err(super::super::storage)?,
                b"captured"
            );
            let snapshot = harness
                .task_files()
                .snapshot(&crate::task_volume::Query::Tasks, 16)
                .await?;
            let value = snapshot
                .files
                .iter()
                .find_map(|file| match &file.key {
                    crate::task_volume::FileKey::Task {
                        file:
                            crate::task_volume::TaskFile::Tool {
                                file: crate::task_volume::ToolFile::Result,
                                ..
                            },
                        ..
                    } => Some(&file.bytes),
                    _ => None,
                })
                .ok_or_else(|| Error::NotFound("bash terminal file".into()))?;
            let result: Value =
                serde_json::from_slice(value.as_ref()).map_err(super::super::storage)?;
            assert_eq!(result["exit_code"], json!(code));
            assert_eq!(result["success"], false);
            if timeout {
                assert_eq!(
                    result["stop"]["kind"],
                    serde_json::to_value(crate::filesystem::NativeProcessStopKind::Timeout)
                        .map_err(super::super::storage)?
                );
            } else {
                assert_eq!(result["stdout"], "stdout");
                assert_eq!(result["stderr"], "stderr");
            }
            Ok::<_, Error>(())
        }))
        .await;
    for outcome in outcomes {
        outcome?;
    }
    Ok(())
}

#[tokio::test]
async fn bash_output_is_live_before_exit_and_replays_without_relaunch() -> Result<()> {
    use crate::task_volume::{Component, FileKey, Query, TaskFile, ToolFile};
    use futures::StreamExt as _;
    use std::io::Write as _;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let control = tempfile::tempdir().map_err(super::super::storage)?;
    let pipe = control.path().join("ack");
    let command = format!(
        "mkfifo '{}'; printf visible; IFS= read -r acknowledgement < '{}'; printf final > result",
        pipe.display(),
        pipe.display()
    );
    let provider = scripted([
        input_call(json!({"command":command,"timeout":5})),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model()?, provider.clone()),
    ))
    .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) = harness
        .submit(OperationId::new(), "stream before exit")
        .await?
    else {
        return Err(Error::Conflict("stock admission rejected".into()));
    };
    let query = Query::Task(Component::new(task.to_string())?);
    let mut observation = harness
        .task_files()
        .observe(query.clone(), None, 16)
        .await?;
    let acknowledge = async {
        while let Some(change) = observation.changes.next().await {
            let Some(file) = change?.file else {
                continue;
            };
            if matches!(
                &file.key,
                FileKey::Task {
                    file: TaskFile::Tool {
                        file: ToolFile::Stdout(0),
                        ..
                    },
                    ..
                }
            ) {
                assert_eq!(file.bytes.as_ref(), b"visible");
                assert!(!directory.path().join("result").exists());
                tokio::task::spawn_blocking(move || {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(pipe)?
                        .write_all(b"continue\n")
                })
                .await
                .map_err(super::super::storage)?
                .map_err(super::super::storage)?;
                return Ok::<_, Error>(());
            }
        }
        Err(Error::NotFound("live bash output".into()))
    };
    let commands = harness.commands()?;
    let checked = super::checked_commands::Checked(&commands);
    let (advanced, acknowledged) = tokio::time::timeout(std::time::Duration::from_secs(8), async {
        tokio::join!(harness.advance(task, &checked), acknowledge)
    })
    .await
    .map_err(super::super::storage)?;
    advanced?;
    acknowledged?;
    harness.resume(task).await?;
    let snapshot = harness.task_files().snapshot(&query, 16).await?;
    assert!(snapshot.files.iter().any(|file| matches!(
        &file.key,
        FileKey::Task {
            file: TaskFile::Tool {
                file: ToolFile::Result,
                ..
            },
            ..
        }
    )));
    assert_eq!(
        std::fs::read(directory.path().join("result")).map_err(super::super::storage)?,
        b"final"
    );
    let expected: Vec<_> = snapshot
        .files
        .into_iter()
        .map(|file| (file.key, file.channel, file.bytes.as_ref().to_vec()))
        .collect();
    drop(observation);
    drop(commands);
    drop(harness);
    let reopened = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model()?, provider.clone()),
    ))
    .await?;
    let actual: Vec<_> = reopened
        .task_files()
        .snapshot(&query, 16)
        .await?
        .files
        .into_iter()
        .map(|file| (file.key, file.channel, file.bytes.as_ref().to_vec()))
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}
