use super::*;
use crate::{
    conversation::ContentPublisher,
    distributed::{WorkPull, Worker},
    runtime::{RuntimeScope, RuntimeTask, TaskSpawner},
    scheduler::LeaseFence,
};

#[cfg(all(feature = "native-execution", unix))]
#[tokio::test]
async fn child_pre_tool_hook_uses_its_own_checkout_and_grants() -> Result<()> {
    use crate::task_volume::{ChildFile, Component, FileKey, FileValue, Query};
    use acyclic_fs::facade::FsWork as _;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::write(directory.path().join("source.txt"), b"parent source")
        .map_err(super::super::storage)?;
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "child-call".into(),
                "bash".into(),
                json!({"command":"printf tool > tool.txt"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]),
        ScriptedTurn::Text("child done".into()),
    ]);
    let home = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir_all(home.path().join(".claude")).map_err(super::super::storage)?;
    std::fs::write(home.path().join(".claude/settings.json"), json!({"hooks":{
        "PreToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"sh",
            "args":["-c","cat > hook-input.json; printf pre > hook.txt"],"onFailure":"block"}]}],
        "PostToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"sh",
            "args":["-c","cat > post-input.json; printf post > post.txt"],"onFailure":"block"}]}]
    }}).to_string()).map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model()?, provider.clone()))?
            .settings(
                crate::native_settings::SettingsConfig::new(directory.path(), home.path())
                    .codex_home(home.path().join(".codex")),
            ),
    )
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission");
    };
    let worker = Worker {
        id: "lazy-child-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease");
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let context = harness.runtime.task_context(parent, fence.clone()).await?;
    let files = harness.task_files();
    let reference = Component::new("worker").map_err(super::super::storage)?;
    let mut request = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        },
        b"inspect the child files".to_vec(),
    );
    let snapshot = files
        .observe(Query::Child(reference.clone()), None, 8)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    let (condition, deadline) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    files
        .write_if(snapshot.generation, &request, parent, condition, deadline)
        .await
        .map_err(super::super::storage)?;
    request.requester = Some(parent);
    let (child, view) = harness.admit_child_request(files, &request).await?;
    assert_ne!(
        view.workspace().id(),
        harness.workspace().unwrap().view().workspace().id()
    );
    assert!(provider.requests().is_empty());
    let admitted = host.observe_admission(child).await?;
    assert_eq!(admitted.parent, Some(parent));
    assert!(!admitted.resource_proofs.is_empty());
    assert!(
        !admitted.grants.contains(
            &harness
                .project()
                .capability(VolumeOperation::Write, &mut None)?
        )
    );
    assert!(
        !admitted
            .grants
            .contains(&harness.project().directory_read_capability("", &mut None)?)
    );
    let WorkPull::Claimed(lease) = host
        .pull_work(&worker, Some(admitted.operation_id), &mut None)
        .await?
    else {
        panic!("child lease");
    };
    host.start_task(&lease, &mut None).await?;
    let child_context = harness
        .runtime
        .task_context(child, LeaseFence::from(&lease.reservation))
        .await?;
    let command: crate::workflow::WorkflowCommand =
        serde_json::from_value(admitted.input["model"].clone()).map_err(super::super::storage)?;
    let bytes = child_context.read_file(&command.payload, None).await?;
    let model: crate::filesystem::ModelTaskCommand =
        crate::contract::json_from_slice(&bytes).map_err(super::super::storage)?;
    let project = model.selected_output_volume.unwrap();
    child_context
        .stage_file(
            Some(&project),
            OperationId::new(),
            "child.txt",
            b"child only",
            "text/plain",
            "child.txt",
        )
        .await?;
    assert!(!directory.path().join("child.txt").exists());
    let cancel = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut accounting = budget.accounting(&cancel);
    assert_eq!(
        view.read("/source.txt", 1024, &mut accounting)
            .await
            .map_err(super::super::storage)?
            .bytes
            .as_ref(),
        b"parent source"
    );
    assert_eq!(
        view.read("/child.txt", 1024, &mut accounting)
            .await
            .map_err(super::super::storage)?
            .bytes
            .as_ref(),
        b"child only"
    );
    let publisher = harness.input_publisher_with_limit(harness.limits.file_bytes)?;
    let message = OperationId::new();
    let (payload, _) = publisher
        .stage_once(
            message,
            "inputs/lazy-child-request.md",
            b"inspect the child files",
            "text/plain",
            "request.md",
            None,
        )?
        .await?;
    host.send_mail(parent, &fence, child, message, payload, None)
        .await?;
    harness.advance(child, &harness.commands()?).await?;
    assert_eq!(provider.requests().len(), 2);
    for (path, expected) in [
        ("/hook.txt", b"pre".as_slice()),
        ("/post.txt", b"post".as_slice()),
        ("/tool.txt", b"tool".as_slice()),
    ] {
        assert_eq!(
            view.read(path, 4096, &mut accounting)
                .await
                .map_err(super::super::storage)?
                .bytes
                .as_ref(),
            expected
        );
        assert!(!directory.path().join(&path[1..]).exists());
    }
    let input = view
        .read("/hook-input.json", 4096, &mut accounting)
        .await
        .map_err(super::super::storage)?;
    let input: Value = serde_json::from_slice(&input.bytes).map_err(super::super::storage)?;
    assert_eq!(input["tool_use_id"], "child-call");
    host.cancel(child).await?;
    host.cancel(parent).await?;
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn child_request_prepares_a_replayable_lazy_fork_and_runs_under_its_original_owner()
-> Result<()> {
    use crate::task_volume::{ChildFile, Component, FileKey, FileValue, Query};
    use acyclic_fs::facade::FsWork as _;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::write(directory.path().join("source.txt"), b"parent source")
        .map_err(super::super::storage)?;
    let provider = scripted([ScriptedTurn::Text("child done".into())]);
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission");
    };
    let worker = Worker {
        id: "lazy-child-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease");
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let context = harness.runtime.task_context(parent, fence.clone()).await?;
    let files = harness.task_files();
    let reference = Component::new("worker").map_err(super::super::storage)?;
    let mut request = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        },
        b"inspect the child files".to_vec(),
    );
    let snapshot = files
        .observe(Query::Child(reference.clone()), None, 8)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    let (condition, deadline) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    files
        .write_if(snapshot.generation, &request, parent, condition, deadline)
        .await
        .map_err(super::super::storage)?;
    request.requester = Some(parent);
    let (child, view) = harness.admit_child_request(files, &request).await?;
    assert_ne!(
        view.workspace().id(),
        harness.workspace().unwrap().view().workspace().id()
    );
    assert!(provider.requests().is_empty());
    let admitted = host.observe_admission(child).await?;
    assert_eq!(admitted.parent, Some(parent));
    assert!(!admitted.resource_proofs.is_empty());
    assert!(
        !admitted.grants.contains(
            &harness
                .project()
                .capability(VolumeOperation::Write, &mut None)?
        )
    );
    assert!(
        !admitted
            .grants
            .contains(&harness.project().directory_read_capability("", &mut None)?)
    );
    let WorkPull::Claimed(lease) = host
        .pull_work(&worker, Some(admitted.operation_id), &mut None)
        .await?
    else {
        panic!("child lease");
    };
    host.start_task(&lease, &mut None).await?;
    let child_context = harness
        .runtime
        .task_context(child, LeaseFence::from(&lease.reservation))
        .await?;
    let command: crate::workflow::WorkflowCommand =
        serde_json::from_value(admitted.input["model"].clone()).map_err(super::super::storage)?;
    let bytes = child_context.read_file(&command.payload, None).await?;
    let model: crate::filesystem::ModelTaskCommand =
        crate::contract::json_from_slice(&bytes).map_err(super::super::storage)?;
    let project = model.selected_output_volume.unwrap();
    child_context
        .stage_file(
            Some(&project),
            OperationId::new(),
            "child.txt",
            b"child only",
            "text/plain",
            "child.txt",
        )
        .await?;
    assert!(!directory.path().join("child.txt").exists());
    let cancel = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut accounting = budget.accounting(&cancel);
    assert_eq!(
        view.read("/source.txt", 1024, &mut accounting)
            .await
            .map_err(super::super::storage)?
            .bytes
            .as_ref(),
        b"parent source"
    );
    assert_eq!(
        view.read("/child.txt", 1024, &mut accounting)
            .await
            .map_err(super::super::storage)?
            .bytes
            .as_ref(),
        b"child only"
    );
    // Property: child checkout authority is exactly its sealed project binding plus
    // the tool grant and original call identity, even after scope attenuation.
    {
        use proptest::prelude::*;
        let child_context = Arc::new(child_context);
        let read = project.capability(VolumeOperation::Read, &mut None)?;
        let write = project.capability(VolumeOperation::Write, &mut None)?;
        proptest::test_runner::TestRunner::new(ProptestConfig {
            source_file: Some(file!()),
            ..ProptestConfig::with_cases(32)
        })
        .run(&(any::<[u8; 16]>(), "[a-z]{1,16}"), |(bytes, call)| {
            for name in ["fs", crate::tools::shell::PLATFORM.name] {
                let tool = harness.tools().get(name, None, None).unwrap().unwrap();
                let tool_grant = format!("tool:call:{name}");
                for mask in 0u8..32 {
                    let grants = [read.as_str(), write.as_str(), tool_grant.as_str()]
                        .into_iter()
                        .enumerate()
                        .filter(|(index, _)| mask & (1 << index) != 0)
                        .map(|(_, grant)| grant.to_owned());
                    let narrowed = child_context
                        .scoped(
                            crate::Capabilities::new(grants),
                            child_context.scope().limits(),
                        )
                        .unwrap();
                    let context = ToolContext::new(
                        Arc::new(narrowed),
                        OperationId::from_bytes(bytes),
                        &call,
                        None,
                    )
                    .unwrap();
                    let mut actual = bytes;
                    if mask & 8 == 0 {
                        actual[0] ^= 1;
                    }
                    let invocation = ToolInvocation {
                        operation_id: OperationId::from_bytes(actual),
                        call_id: if mask & 16 != 0 {
                            call.clone()
                        } else {
                            format!("{call}-other")
                        },
                        name: name.into(),
                        arguments: json!({"command":"status"}),
                    };
                    prop_assert_eq!(
                        tool.executor
                            .authorize(None, Some(&context), &invocation, None)
                            .is_ok(),
                        mask == 31
                    );
                }
            }
            Ok(())
        })
        .map_err(super::super::storage)?;
    }
    let publisher = harness.input_publisher_with_limit(harness.limits.file_bytes)?;
    let message = OperationId::new();
    let (payload, _) = publisher
        .stage_once(
            message,
            "inputs/lazy-child-request.md",
            b"inspect the child files",
            "text/plain",
            "request.md",
            None,
        )?
        .await?;
    host.send_mail(parent, &fence, child, message, payload, None)
        .await?;
    harness.advance(child, &harness.commands()?).await?;
    assert_eq!(provider.requests().len(), 1);
    assert!(host.outcome(child).await?.is_none());
    let (replayed, reopened) = harness.admit_child_request(files, &request).await?;
    assert_eq!(replayed, child);
    assert_eq!(reopened.workspace().id(), view.workspace().id());
    let mut changed = request.clone();
    changed.bytes = acyclic_stream::OwnedBytes::from_static(b"forged replacement");
    assert!(harness.admit_child_request(files, &changed).await.is_err());
    host.cancel(child).await?;
    host.cancel(parent).await?;
    Ok(())
}

#[tokio::test]
async fn child_agent_keeps_its_admission_and_waits_after_each_received_turn() -> Result<()> {
    // Every wait below is bounded by readiness, not time: the scripted model
    // fails once exhausted, and advance returns when no authenticated wake
    // makes the child claimable (the second advance of each input proves it).
    // A wall-clock deadline here only raced host load in the full suite.
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("first child turn".into()),
        ScriptedTurn::Text("second child turn".into()),
    ]);
    let harness =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    let parent_operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(parent_operation, "parent").await?
    else {
        panic!("parent admission");
    };
    let worker = Worker {
        id: "child-agent-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host
        .pull_work(&worker, Some(parent_operation), &mut None)
        .await?
    else {
        panic!("parent lease");
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let admitted = host.observe_admission(parent).await?;
    let scope = RuntimeScope::new(admitted.grants, admitted.limits)?
        .with_run_limits(admitted.run_limits, &mut None)?;
    let operation = OperationId::new();
    let (receive, model) = harness
        .stage_turn_with_limits(
            operation,
            "follow the received child request",
            scope.limits(),
        )
        .await?;
    let definition = harness
        .runtime
        .harness()
        .task::<serde_json::Value, serde_json::Value>("acyclic.child_agent@1")?;
    let record = harness
        .runtime
        .harness()
        .registered_admission(
            operation,
            &definition.identity().name,
            &definition.identity().version,
            serde_json::json!({"receive":receive,"model":model}),
            Some((parent, &scope)),
            None,
            None,
        )?
        .0;
    let Admission::Accepted(child) = host.admit_owned(record, fence.clone()).await? else {
        panic!("child admission");
    };
    let publisher = crate::filesystem::FilesystemContentPublisher::new(
        harness.files.clone(),
        &harness.journal,
        &harness.verifier,
        &harness.signed,
        scope.limits().file_bytes,
        None,
    )?;
    use crate::runtime::TaskStateProvider as _;
    for (index, text) in [(1, "first request"), (2, "second request")] {
        let message = OperationId::new();
        let (payload, _) = publisher
            .stage_once(
                message,
                &format!("inputs/child-agent/{index}.md"),
                text.as_bytes(),
                "text/plain",
                "request.md",
                None,
            )?
            .await?;
        host.send_mail(parent, &fence, child, message, payload, None)
            .await?;
        harness.advance(child, &harness.commands()?).await?;
        let outcome = host.outcome(child).await?;
        assert!(
            outcome.is_none(),
            "child stopped after input {index}: {outcome:?}"
        );
        assert_eq!(provider.requests().len(), index);
        if index == 1 {
            let (hidden, _) = publisher
                .stage_once(
                    OperationId::new(),
                    "private/unreadable.json",
                    b"1",
                    "application/json",
                    "private.json",
                    None,
                )?
                .await?;
            let slot = host.workflow_suspension(child, &mut None).await?.unwrap();
            assert!(matches!(
                host.resume_workflow(
                    child,
                    slot.revision,
                    hidden,
                    crate::IdempotencyKey::new("unreadable-wake")?,
                    slot.waiting_command,
                    &mut None,
                )
                .await,
                Err(crate::Error::Unauthorized(_))
            ));
            let (readable, _) = publisher
                .stage_once(
                    OperationId::new(),
                    "inputs/readable-wake.json",
                    b"1",
                    "application/json",
                    "readable.json",
                    None,
                )?
                .await?;
            host.resume_workflow(
                child,
                slot.revision,
                readable,
                crate::IdempotencyKey::new("readable-wake")?,
                slot.waiting_command,
                &mut None,
            )
            .await?;
            let WorkPull::Claimed(resumed) =
                host.pull_work(&worker, Some(operation), &mut None).await?
            else {
                panic!("readable input did not wake the child");
            };
            host.start_task(&resumed, &mut None).await?;
            assert_eq!(
                host.workflow_input(child, &LeaseFence::from(&resumed.reservation), None)
                    .await?,
                Some((slot.revision, serde_json::json!(1)))
            );
        }
        harness.advance(child, &harness.commands()?).await?;
        assert_eq!(provider.requests().len(), index);
    }
    host.cancel(child).await?;
    assert!(matches!(
        host.outcome(child).await?,
        Some(crate::Outcome::Cancelled)
    ));
    host.cancel(parent).await?;
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn child_file_admission_replays_under_the_parent_lease_and_shared_depth_limit() -> Result<()>
{
    use crate::task_volume::{ChildFile, ChildRequest, Component, FileKey, FileValue, Query};
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([]);
    let mut config =
        crate::test_support::local_harness_config(directory.path(), (model()?, provider.clone()))?;
    config.session_limits.depth = 1;
    let harness = LocalHarness::open(config).await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission");
    };
    let worker = Worker {
        id: "child-file-admission-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease");
    };
    host.start_task(&lease, &mut None).await?;
    let parent_context = harness
        .runtime
        .task_context(parent, LeaseFence::from(&lease.reservation))
        .await?;
    let request = ChildRequest {
        prompt: "reader".into(),
        model: None,
        tools: Some(vec!["read".into(), "spawn".into()]),
        grants: None,
    };
    let files = Arc::clone(&harness.task_files);
    let reference = Component::new("reader").map_err(super::super::storage)?;
    let file = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        },
        request.encode(),
    );
    let generation = files.inner.head().await.map_err(super::super::storage)?;
    let (condition, deadline) = parent_context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    files
        .write_if(generation, &file, parent, condition, deadline)
        .await
        .map_err(super::super::storage)?;
    let file = files
        .observe(Query::File(file.key), None, 1)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap()
        .files
        .remove(0);
    let (child, view) = harness.admit_child_request(files.as_ref(), &file).await?;
    let (same, replay) = harness.admit_child_request(files.as_ref(), &file).await?;
    assert_eq!(
        (same, replay.workspace().id()),
        (child, view.workspace().id())
    );
    let admitted = host.observe_admission(child).await?;
    assert_eq!(admitted.parent, Some(parent));
    assert_eq!(admitted.limits, parent_context.scope().limits());
    assert_eq!(admitted.run_limits, parent_context.scope().run_limits());
    assert!(admitted.grants.contains("tool:call:read"));
    assert!(admitted.grants.contains("tool:call:spawn"));
    assert!(!admitted.grants.contains("tool:call:write"));
    assert!(
        harness
            .runtime
            .children(parent, None, None, 8)
            .await?
            .entries
            .iter()
            .any(|entry| entry.task_id == child)
    );
    let child_operation = OperationId::from_bytes(child.into_bytes());
    let WorkPull::Claimed(lease) = host
        .pull_work(&worker, Some(child_operation), &mut None)
        .await?
    else {
        panic!("child lease");
    };
    host.start_task(&lease, &mut None).await?;
    let context = harness
        .runtime
        .task_context(child, LeaseFence::from(&lease.reservation))
        .await?;
    let child_files = files
        .for_created_task(&context, context.scope())
        .await?
        .unwrap();
    let file = FileValue::new(
        FileKey::Child {
            reference: Component::new("too-deep").map_err(super::super::storage)?,
            file: ChildFile::Request,
        },
        b"too deep".to_vec(),
    );
    let generation = child_files
        .inner
        .head()
        .await
        .map_err(super::super::storage)?;
    let (condition, deadline) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    child_files
        .write_if(generation, &file, child, condition, deadline)
        .await
        .map_err(super::super::storage)?;
    let file = child_files
        .observe(Query::File(file.key), None, 1)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap()
        .files
        .remove(0);
    assert!(
        matches!(harness.admit_child_request(&child_files, &file).await,
        Err(Error::Invalid(reason)) if reason.contains("depth limit exhausted"))
    );
    assert!(
        harness
            .runtime
            .children(child, None, None, 8)
            .await?
            .entries
            .is_empty()
    );
    assert!(provider.requests().is_empty());
    Ok(())
}
