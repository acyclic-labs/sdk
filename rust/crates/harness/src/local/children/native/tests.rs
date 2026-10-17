use super::*;
use crate::{
    distributed::{WorkPull, Worker},
    external_executor::{ExternalHost, ExternalRequest, RequestConsumer, request, tool_result},
    fork::ForkRequest,
    runtime::TaskStateProvider,
    task_volume::Component,
    workflow::WorkflowCommand,
};
use acyclic_fs::facade::FsWork as _;
use proptest::prelude::*;
use serde_json::json;

#[tokio::test]
async fn native_depth_rejection_is_terminal_and_keeps_the_original_session_usable() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let mut config = LocalHarnessConfig::external(directory.path());
    config.session_limits.depth = 1;
    let harness = LocalHarness::open(config).await?;
    let event = |name, input| request(ExternalHost::Pi, name, input);
    let parent = event(
        "session_start",
        json!({"sessionId":"depth","cwd":directory.path()}),
    )?;
    harness.external_request(&parent).await?;
    let fork = event(
        "subagent_start",
        json!({"sessionId":"depth","agentId":"child"}),
    )?;
    let child = event(
        "session_start",
        json!({"sessionId":"depth","agentId":"child","cwd":directory.path()}),
    )?;
    assert_eq!(
        harness.external_request(&fork).await?,
        json!({"Succeeded":child.task_id()})
    );
    harness.external_request(&child).await?;
    let nested = event(
        "subagent_start",
        json!({"sessionId":"depth","parentAgentId":"child","agentId":"too-deep"}),
    )?;
    let rejected = harness.external_request(&nested).await?;
    assert!(
        rejected["Failed"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("depth limit exhausted")),
        "{rejected}"
    );
    assert_eq!(harness.external_request(&nested).await?, rejected);
    assert!(
        harness
            .runtime
            .children(child.task_id(), None, None, 8)
            .await?
            .entries
            .is_empty()
    );
    let inspect = event(
        "tool_call",
        json!({"sessionId":"depth","agentId":"child","toolCallId":"after-rejection",
        "toolName":"acyclic_inspect","input":{"task":child.task_id()}}),
    )?;
    let inspected = harness.external_request(&inspect).await?;
    assert_eq!(inspected["Succeeded"]["task"], json!(child.task_id()));
    harness.close().await
}

#[tokio::test]
async fn native_preparation_reconciles_its_staged_cut_before_admission() -> Result<()> {
    use crate::distributed::SchedulerPayloadStore;
    use crate::external_executor::LIFECYCLE;
    let directory = tempfile::tempdir().map_err(storage)?;
    let harness = LocalHarness::open(LocalHarnessConfig::external(directory.path())).await?;
    let event = |name, input| request(ExternalHost::Pi, name, input);
    let parent = event(
        "session_start",
        json!({"sessionId":"recover-native","cwd":directory.path()}),
    )?;
    harness.external_request(&parent).await?;
    let write = |call, text| {
        event(
            "tool_call",
            json!({"sessionId":"recover-native","toolCallId":call,
        "toolName":"write","input":{"path":"source.txt","text":text,"media_type":"text/plain","display_name":"source.txt"}}),
        )
    };
    tool_result(
        harness
            .external_request(&write("before", "original cut")?)
            .await?,
    )?;
    let host = harness.runtime.task_host();
    let (context, fence) = claim_request(&harness, &parent, "before-fork").await?;
    let child = event(
        "session_start",
        json!({"sessionId":"recover-native","agentId":"child","cwd":directory.path()}),
    )?;
    let publisher = harness.input_publisher_with_limit(harness.limits.file_bytes)?;
    let reference = Component::new(child.task_id().to_string()).map_err(storage)?;
    let file = child.to_file(reference.clone()).map_err(storage)?;
    let payload = publisher
        .stage_input(
            child.operation_id,
            &format!("external-command:{}", child.operation_id),
            &file.bytes,
            None,
        )?
        .await?;
    let command = WorkflowCommand {
        operation_id: child.operation_id,
        kind: LIFECYCLE.into(),
        payload: payload.clone(),
    };
    let template = harness
        .runtime
        .harness()
        .registered_admission(
            child.operation_id,
            "acyclic.command_session",
            "1",
            json!({"command":command,"terminal":false}),
            Some((parent.task_id(), context.scope())),
            None,
            None,
        )?
        .0;
    let inputs = vec![payload];
    super::super::cuts::CUT.with(|cut| cut.set(Some("inputs")));
    let interrupted = harness
        .admit(
            &context,
            &fence,
            &reference,
            template.clone(),
            inputs.clone(),
            false,
        )
        .await;
    assert!(
        super::super::cuts::CUT.with(|cut| cut.replace(None).is_none()),
        "input cut was not reached"
    );
    assert!(matches!(interrupted,Err(Error::Indeterminate(id)) if id == child.operation_id));
    let phase = |name| {
        admission::child_operation(
            harness.task_files.volume(),
            &child.operation_id.to_string(),
            name,
        )
    };
    assert!(
        host.reconcile_admission(phase(b"preparation")?)
            .await?
            .is_none()
    );
    let checkpoint = publisher
        .observe_staged_once(phase(b"source")?, harness.limits.file_bytes, None)?
        .await?
        .0
        .unwrap();
    tool_result(
        harness
            .external_request(&write("after", "later parent")?)
            .await?,
    )?;
    let (context, fence) = claim_request(&harness, &parent, "recover-fork").await?;
    assert_eq!(
        harness
            .admit(
                &context,
                &fence,
                &reference,
                template.clone(),
                inputs.clone(),
                true
            )
            .await?,
        child.task_id()
    );
    assert_eq!(
        publisher
            .observe_staged_once(phase(b"source")?, harness.limits.file_bytes, None)?
            .await?
            .0,
        Some(checkpoint)
    );
    let task = harness
        .admit_child(
            &harness.task_files,
            &reference,
            parent.task_id(),
            admission::ChildInput::Native {
                context: &context,
                fence: &fence,
                child: template,
                inputs,
                reconcile: true,
            },
        )
        .await?;
    assert_eq!(task, child.task_id());
    let view = harness.child_project_view(task).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut work = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
    assert_eq!(
        view.read("/source.txt", 1024, &mut work)
            .await
            .map_err(storage)?
            .bytes
            .as_ref(),
        b"original cut"
    );
    assert_eq!(
        std::fs::read(directory.path().join("source.txt")).map_err(storage)?,
        b"later parent"
    );
    drop(work);
    drop(view);
    harness.close().await
}

#[tokio::test]
async fn native_children_keep_their_identity_and_publish_only_into_their_prepared_fork()
-> Result<()> {
    tokio::try_join!(
        Box::pin(isolated_native_child(ExternalHost::Codex)),
        Box::pin(isolated_native_child(ExternalHost::ClaudeCode)),
        Box::pin(isolated_native_child(ExternalHost::Pi)),
    )?;
    Ok(())
}

async fn isolated_native_child(host: ExternalHost) -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    std::fs::write(directory.path().join("source.txt"), b"parent source").map_err(storage)?;
    let harness = LocalHarness::open(LocalHarnessConfig::external(directory.path())).await?;
    let pi = host == ExternalHost::Pi;
    let session = host.name();
    let event = |name, input| request(host, name, input);
    let parent = event(
        if pi { "session_start" } else { "SessionStart" },
        json!({"sessionId":session,"cwd":directory.path()}),
    )?;
    harness.external_request(&parent).await?;
    let fork = event(
        if pi {
            "subagent_start"
        } else {
            "SubagentStart"
        },
        json!({"sessionId":session,"agentId":"worker"}),
    )?;
    let child = event(
        if pi { "session_start" } else { "SessionStart" },
        json!({"sessionId":session,"agentId":"worker","cwd":directory.path()}),
    )?;
    let answer = harness.external_request(&fork).await?;
    assert_eq!(answer, json!({"Succeeded":child.task_id()}));
    assert_eq!(harness.external_request(&fork).await?, answer);
    harness.external_request(&child).await?;
    let filename = format!("{}-child.txt", host.name());
    let write = event(
        if pi { "tool_call" } else { "PreToolUse" },
        json!({
            "sessionId":session,"agentId":"worker","toolCallId":"write-child",
            "toolName":"write","input":{"path":filename,"text":"child only",
                "media_type":"text/plain","display_name":"child.txt"},
        }),
    )?;
    let written = harness.external_request(&write).await?;
    if host == ExternalHost::Pi {
        let crate::external_executor::ExternalAction::RunTool { arguments, .. } = &write.action
        else {
            unreachable!()
        };
        let mut arguments = arguments.clone();
        let task = Component::new(write.task_id().to_string()).map_err(storage)?;
        let path = write.request_key(task).map_err(storage)?.path();
        let mut workspace = blake3::hash(
            harness
                .workspace_path()
                .unwrap()
                .as_os_str()
                .as_encoded_bytes(),
        )
        .to_hex()
        .to_string();
        arguments["_acyclic"] = json!(format!("workspaces/{workspace}/{path}"));
        assert_eq!(
            harness
                .external_tool_result("write", arguments.clone())
                .await?,
            written
        );
        workspace.replace_range(..1, if workspace.starts_with('a') { "b" } else { "a" });
        arguments["_acyclic"] = json!(format!("workspaces/{workspace}/{path}"));
        assert!(matches!(
            harness.external_tool_result("write", arguments).await,
            Err(Error::Unauthorized(_))
        ));
    }
    let result = tool_result(written.clone())?;
    let file: crate::conversation::FileRef =
        serde_json::from_value(result["details"]["file"].clone()).map_err(storage)?;
    assert_eq!(file.volume().class(), VolumeClass::Project);
    assert_ne!(file.volume(), harness.project());
    assert_eq!(harness.external_request(&write).await?, written);
    assert!(
        !directory.path().join(&filename).exists(),
        "child wrote into parent checkout"
    );
    assert_eq!(
        std::fs::read(directory.path().join("source.txt")).map_err(storage)?,
        b"parent source"
    );
    let diff = event(
        if pi { "tool_call" } else { "PreToolUse" },
        json!({
            "sessionId":session,"toolCallId":"diff-child","toolName":"fs",
            "input":{"command":format!("diff agents/{}",child.task_id())},
        }),
    )?;
    let diff = tool_result(harness.external_request(&diff).await?)?;
    let command: acyclic_fs::GitCommandOutput =
        serde_json::from_value(diff["details"]["result"].clone()).map_err(storage)?;
    let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
        kind,
        value,
    }) = command
    else {
        panic!("expected a published child branch diff: {diff}");
    };
    assert_eq!(kind, "diff");
    assert!(value["fileChanges"].as_u64().unwrap() > 0, "{diff}");
    assert_project_binding_property(&harness, &child, &file).await?;

    let completed = event(
        if pi { "session_shutdown" } else { "SessionEnd" },
        json!({"sessionId":session,"agentId":"worker"}),
    )?;
    harness.external_request(&completed).await?;
    if host == ExternalHost::Pi {
        assert_native_template_property(&harness, &parent, child.task_id()).await?;
    }
    harness.close().await
}

async fn assert_project_binding_property(
    harness: &LocalHarness,
    child: &ExternalRequest,
    file: &crate::conversation::FileRef,
) -> Result<()> {
    let (context, _) = claim_request(harness, child, "project-property").await?;
    assert_eq!(context.read_file(file, None).await?.as_ref(), b"child only");
    let project = context
        .scope()
        .created_access
        .as_ref()
        .unwrap()
        .content
        .iter()
        .find(|binding| binding.volume.class() == VolumeClass::Project)
        .unwrap()
        .volume
        .clone();
    let write = project.capability(VolumeOperation::Write, &mut None)?;
    assert_eq!(
        context
            .content_binding(None)
            .unwrap()
            .writer
            .as_ref()
            .unwrap()
            .volume()
            .class(),
        VolumeClass::AgentPrivate
    );
    proptest::test_runner::TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        ..ProptestConfig::with_cases(32)
    })
    .run(&(any::<bool>(), any::<u64>()), |(keep_write, mask)| {
        let grants = Capabilities::new(
            context
                .scope()
                .grants()
                .iter()
                .enumerate()
                .filter(|(index, grant)| {
                    if *grant == write {
                        keep_write
                    } else {
                        mask & (1u64 << (index % 64)) != 0
                    }
                })
                .map(|(_, grant)| grant),
        );
        let selected = context
            .scoped(grants.clone(), context.scope().limits())?
            .with_created_project_output();
        prop_assert_eq!(selected.is_ok(), keep_write);
        if let Ok(selected) = selected {
            prop_assert_eq!(selected.scope().grants(), &grants);
            prop_assert_eq!(selected.scope().limits(), context.scope().limits());
            prop_assert_eq!(selected.scope().run_limits(), context.scope().run_limits());
            prop_assert_eq!(
                selected
                    .content_binding(None)
                    .unwrap()
                    .writer
                    .as_ref()
                    .unwrap()
                    .volume(),
                &project
            );
            prop_assert_eq!(
                selected
                    .scope()
                    .created_access
                    .as_ref()
                    .unwrap()
                    .content
                    .iter()
                    .find(|binding| binding.volume.class() == VolumeClass::AgentPrivate)
                    .unwrap()
                    .binding
                    .writer
                    .as_ref()
                    .unwrap()
                    .volume()
                    .class(),
                VolumeClass::AgentPrivate
            );
        }
        Ok(())
    })
    .map_err(storage)
}

async fn assert_native_template_property(
    harness: &LocalHarness,
    parent: &ExternalRequest,
    child: TaskId,
) -> Result<()> {
    let host = harness.runtime.task_host();
    let (context, fence) = claim_request(harness, parent, "template-property").await?;
    let parent = parent.task_id();
    let admitted = host.observe_admission(child).await?;
    let initial: WorkflowCommand =
        serde_json::from_value(admitted.input["command"].clone()).map_err(storage)?;
    let inputs = vec![initial.payload];
    let mut template = harness
        .runtime
        .harness()
        .registered_admission(
            admitted.operation_id,
            &admitted.task.name,
            &admitted.task.version,
            admitted.input.clone(),
            Some((parent, context.scope())),
            None,
            None,
        )?
        .0;
    template.resource_proofs.clear();
    let operation = admission::child_operation(
        harness.task_files.volume(),
        &admitted.operation_id.to_string(),
        b"preparation",
    )?;
    let (_, retained) = host
        .reconcile_admission(operation)
        .await?
        .expect("retained preparation");
    let prepared: ResourcePreparationRequest =
        serde_json::from_value(retained.input).map_err(storage)?;
    let fork: ForkRequest = serde_json::from_value(prepared.payload.clone()).map_err(storage)?;
    let project = &fork.preparation.child_project_volume;
    let private = &fork.preparation.child_private_volume;
    let volumes = [&harness.project, &harness.scratch, project, private];
    // Property: only the exact original native template and selected input set
    // recover the sealed child; changed identity, authority or inputs never do.
    proptest::test_runner::TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        ..ProptestConfig::with_cases(32)
    })
    .run(&(any::<[u8; 16]>(), "[a-z]{1,16}"), |(id, text)| {
        for change in 0..8 {
            let mut candidate = template.clone();
            let mut files = inputs.clone();
            match change {
                0 => {}
                1 => candidate.operation_id = OperationId::from_bytes(id),
                2 => candidate.input["terminal"] = json!(true),
                3 => candidate.parent = None,
                4 => candidate.grants = Capabilities::default(),
                5 => candidate.limits.file_bytes /= 2,
                6 => files.clear(),
                _ => candidate.input["command"]["kind"] = json!(text),
            }
            let unchanged = candidate == template && files == inputs;
            prop_assert_eq!(
                verify_retained(&context, &candidate, &files, &admitted, &prepared, volumes)
                    .is_ok(),
                unchanged
            );
        }
        Ok(())
    })
    .map_err(storage)?;
    let reference = Component::new(child.to_string()).map_err(storage)?;
    let future = harness.admit(
        &context,
        &fence,
        &reference,
        template.clone(),
        inputs.clone(),
        false,
    );
    let size = std::mem::size_of_val(&future);
    eprintln!("native child admission future: {size} bytes");
    assert!(size < 16 * 1024);
    assert_eq!(future.await?, child);
    host.cancel(parent).await?;
    assert!(
        harness
            .admit(
                &context,
                &fence,
                &reference,
                template.clone(),
                inputs.clone(),
                false
            )
            .await
            .is_err(),
        "cancelled parent cannot dispatch a fresh child admission"
    );
    assert_eq!(
        harness
            .admit(&context, &fence, &reference, template, inputs, true)
            .await?,
        child,
        "cancellation must still allow settlement of the retained child identity"
    );
    Ok(())
}

// Claim an admitted request as the real worker does. An empty suspended native
// session deliberately has no reservation to recover.
async fn claim_request(
    harness: &LocalHarness,
    attachment: &ExternalRequest,
    call: &str,
) -> Result<(TaskContext, LeaseFence)> {
    let inspect = request(
        attachment.host,
        if attachment.host == ExternalHost::Pi {
            "tool_call"
        } else {
            "PreToolUse"
        },
        json!({"sessionId":attachment.session_id,"agentId":attachment.caller,
            "toolCallId":call,"toolName":"acyclic_inspect","input":{"task":attachment.task_id()}}),
    )?;
    assert_eq!(inspect.task_id(), attachment.task_id());
    let task = Component::new(inspect.task_id().to_string()).map_err(storage)?;
    let volumes = harness.external_volumes(inspect.task_id()).await?;
    let file = inspect.to_file(task.clone()).map_err(storage)?;
    crate::external_executor::publish_request(&volumes.requests, &file, None, None)
        .await
        .map_err(storage)?;
    let publisher = harness.input_publisher_with_limit(harness.limits.file_bytes)?;
    RequestConsumer::new(
        &harness.runtime,
        &volumes.requests,
        &volumes.results,
        publisher.as_ref(),
        "",
    )
    .accept(&task, &file)
    .await?;
    let host = harness.runtime.task_host();
    let worker = Worker {
        id: "native-preparation-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let operation = OperationId::from_bytes(inspect.task_id().into_bytes());
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("accepted native request must be claimable");
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    Ok((
        harness
            .runtime
            .task_context(inspect.task_id(), fence.clone())
            .await?,
        fence,
    ))
}
