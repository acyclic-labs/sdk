//! Stock default commands execute against and publish the same checkout.
use super::*;

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
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
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
    std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
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
    Ok(())
}
