//! Stock default commands execute against and publish the same checkout.
use super::*;
use acyclic_fs::facade::FsWork as _;

use super::checked_commands::call;

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
async fn an_unmounted_scratch_directory_captures_edits_and_additions() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let scratch = directory.path().join(".volumes/scratch");
    std::fs::create_dir_all(&scratch).map_err(super::super::storage)?;
    std::fs::write(scratch.join("existing"), b"base").map_err(super::super::storage)?;
    let provider = scripted([
        call(0, "fs", "status"),
        ScriptedTurn::Text("baseline".into()),
        call(1, "fs", "status"),
        ScriptedTurn::Text("captured".into()),
    ]);
    let fs = crate::tools::defaults()?
        .get("fs", None, None)?
        .unwrap()
        .clone();
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state.path())
            .without_default_tools()
            .tool(fs),
    )
    .await?;
    assert_eq!(harness.run("capture baseline").await?.text, "baseline");
    std::fs::write(scratch.join("existing"), b"changed").map_err(super::super::storage)?;
    std::fs::write(scratch.join("added"), b"new").map_err(super::super::storage)?;
    assert_eq!(harness.run("capture edits").await?.text, "captured");
    let head = harness
        .workspace()
        .unwrap()
        .view()
        .workspace()
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(super::super::storage)?
        .value;
    for (name, expected) in [("existing", b"changed".as_slice()), ("added", b"new")] {
        let bytes = head
            .read(
                &format!("/.volumes/scratch/{name}"),
                64,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(super::super::storage)?
            .value;
        assert_eq!(bytes.as_ref(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn checkout_without_bash_uses_directory_capture_without_mounts() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::write(directory.path().join("note"), b"ordinary").map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, text()))?
            .state_directory(state.path())
            .without_default_tools(),
    )
    .await?;
    let workspace = harness.workspace().unwrap();
    assert_eq!(workspace.mounts.mounts().count(), 1);
    // No scratch volume: the harness's only checkout entry is the
    // incarnation token.
    assert_eq!(
        std::fs::read_dir(directory.path().join(".volumes"))
            .map_err(super::super::storage)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(super::super::storage)?,
        [std::ffi::OsString::from(".incarnation")]
    );
    assert_eq!(
        workspace
            .view()
            .read(
                "/note",
                32,
                &mut acyclic_fs::WorkBudget::UNBOUNDED
                    .accounting(&acyclic_fs::CancellationToken::new())
            )
            .await
            .map_err(super::super::storage)?
            .bytes
            .as_ref(),
        b"ordinary"
    );
    Ok(())
}

#[tokio::test]
async fn default_bash_and_fs_share_the_published_checkout() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state_directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
    let provider = scripted(
        COMMANDS
            .iter()
            .enumerate()
            .map(|(index, (name, command))| call(index, name, command))
            .chain([ScriptedTurn::Text("commands completed".into())]),
    );
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state_directory.path()),
    )
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
    // The client-facing, owner-authorized query sees the same chunk files
    // written by bash; there is no parallel output stream or task-file root.
    let observation = harness
        .task_files()
        .observe(crate::task_volume::Query::Root, None, 128)
        .await?;
    let snapshot = observation.snapshot.expect("initial task-file snapshot");
    let stdout: Vec<_> = snapshot
        .files
        .iter()
        .filter(|file| {
            matches!(
                file.key,
                crate::task_volume::FileKey::Task {
                    file: crate::task_volume::TaskFile::Tool {
                        file: crate::task_volume::ToolFile::Stdout(_),
                        ..
                    },
                    ..
                }
            )
        })
        .flat_map(|file| file.bytes.iter().copied())
        .collect();
    assert_eq!(stdout, b"visible");

    let requests = provider.requests();
    assert_eq!(requests.len(), COMMANDS.len() + 1);
    assert!(requests[0].messages.iter().any(|message| {
        matches!(&message.content, crate::model::ModelContent::Text(text)
            if text.contains(harness.workspace().unwrap().context_line()))
    }));
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
    let state_directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state_directory.path()),
    )
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
    {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "unmounted".into(),
            name: "fs".into(),
            arguments: json!({"command":"status --volume journal"}),
        };
        let context = ToolContext::new(
            Arc::clone(&task_context),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        let tool = harness
            .tools()
            .get("fs", None, None)?
            .ok_or_else(|| Error::NotFound("fs".into()))?;
        let refused = tool
            .executor
            .execute(invocation.clone(), Some(context.clone()))
            .await?;
        assert!(
            refused.value["error"]
                .as_str()
                .unwrap()
                .contains("volume is not mounted")
        );
        // Current visibility cannot prove that a previously dispatched call
        // had no effect. A recorded completion replays through the journal;
        // otherwise reconciliation remains conservative.
        assert!(
            tool.executor
                .reconcile(invocation, Some(context))
                .await?
                .is_none()
        );
    }
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
    let workspace = harness.workspace.as_ref().unwrap();
    let first = workspace
        .executor(
            harness.project(),
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            Arc::clone(harness.task_files()),
        )
        .await?;
    let second = workspace
        .executor(
            harness.project(),
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            Arc::clone(harness.task_files()),
        )
        .await?;
    for (executor, command) in [(first, "checkout -b shared-binding"), (second, "status")] {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: if command == "status" {
                "shared-status"
            } else {
                "shared-checkout"
            }
            .into(),
            name: "fs".into(),
            arguments: json!({"command":command}),
        };
        let context = ToolContext::new(
            Arc::clone(&task_context),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        let result = executor.execute(invocation, Some(context)).await?;
        if command == "status" {
            let output: acyclic_fs::GitCommandOutput =
                serde_json::from_value(result.value["result"].clone())
                    .map_err(super::super::storage)?;
            let acyclic_fs::GitCommandOutput::Status(status) = output else {
                return Err(Error::Invalid("expected selected branch status".into()));
            };
            assert_eq!(status.branch, "shared-binding");
            assert_eq!(
                status.workspace.workspace_id(),
                workspace.current().await.id()
            );
        }
    }
    Ok(())
}
