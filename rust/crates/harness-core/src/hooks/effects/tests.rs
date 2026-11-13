use super::*;
use crate::{
    Capabilities, OperationId, Result,
    conversation::{Limits, VolumeOperation},
    filesystem::MemoryHarnessStorage,
    runtime::{Bindings, RuntimeScope, TaskContext, ToolContext},
};
use acyclic_auth::addressing::AccountId;
use acyclic_stream::{
    AppendRequest, FollowRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProvider as _,
    StreamProviderExt as _,
};
use futures::StreamExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};
#[tokio::test]
async fn followed_hook_records_retain_outcomes_and_do_not_repeat_interrupted_effects()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let records = Arc::new(MemoryStream::default());
    let source = StreamPath::new("events/attention")?;
    let account = AccountId::new(b"test-account")?;
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
    let content = storage.content_bindings();
    let scope = RuntimeScope::new(
        crate::Capabilities::new([
            content
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            content
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        ]),
        crate::conversation::Limits::default(),
    )?;
    let task = || -> crate::Result<TaskContext> {
        Bindings {
            scope: scope.clone(),
            content: Some(content.clone()),
            ..Bindings::local()
        }
        .build()?
        .agent_context(&crate::Capabilities::default(), content.clone())
    };
    let fs = acyclic_fs::Fs::new(
        acyclic_fs::StreamAuthorityStore::new(records.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let cancel = acyclic_fs::CancellationToken::new();
    let watched = fs
        .create_workspace("events", acyclic_fs::WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    let changes = acyclic_fs::session::changes_path(&fs, watched.id().volume_id())?;
    for (event, source, from) in [
        (HookEvent::StreamRecord, source, 0),
        (HookEvent::FileChanged, changes, 1),
    ] {
        let mut request = FollowRequest::new(source.clone(), from);
        request.when_absent = acyclic_stream::WhenAbsent::Wait;
        let mut follow = acyclic_stream::records_only(records.follow(request).await?);
        let workspace = tempfile::tempdir()?;
        let mut hooks = Hooks::default();
        hooks.add_rules([HookRule {
            on: event,
            matcher: String::new(),
            hooks: vec![HookSpec {
                run: HookRun::Shell {
                    command: if cfg!(windows) {
                        "[System.IO.File]::AppendAllText((Join-Path (Get-Location) 'receipt'), '!')"
                    } else {
                        "printf '!' >> receipt"
                    }
                    .into(),
                    shell: if cfg!(windows) {
                        None
                    } else {
                        Some("sh".into())
                    },
                },
                timeout_ms: None,
                failure: HookFailure::Block,
            }],
        }])?;
        let registry = ToolRegistry::default();
        let input = serde_json::json!({"stream_path": source.as_str(), "path": "watched.txt"});
        for interrupted in [false, true] {
            if event == HookEvent::FileChanged {
                watched
                    .write_text("/watched.txt", if interrupted { "second" } else { "first" })
                    .await?;
            } else {
                records
                    .append(AppendRequest {
                        path: source.clone(),
                        records: vec![b"attention".as_slice().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::unique(),
                    })
                    .await?;
            }
            // The producer's own follow wakes this read; no readiness polling.
            let record = follow.next().await.ok_or("missing followed event")??;
            let operation = record_operation(&account, &source, &record, "hooks/notify");
            let context = ToolContext::new(
                Arc::new(task()?.with_execution_journal(storage.journal(), operation)?),
                operation,
                "event",
            )?;
            let competing = ToolContext::new(
                Arc::new(task()?.with_execution_journal(storage.journal(), operation)?),
                operation,
                "event",
            )?;
            let (entered, running) = tokio::sync::oneshot::channel();
            let (resume, ready) = tokio::sync::oneshot::channel();
            let initial = run_once(
                &context,
                event,
                false,
                (
                    || async {
                        entered.send(()).expect("contender remains live");
                        ready.await.expect("contender checked original claim");
                        let output = hooks
                            .run(event, workspace.path(), workspace.path(), &input, &registry)
                            .await?;
                        if interrupted {
                            Err(Error::Storage(
                                "interrupted before outcome publication".into(),
                            ))
                        } else {
                            Ok(output)
                        }
                    },
                    || hooks.interrupted(event, &input, &registry),
                ),
            );
            let contender = async {
                running.await.expect("producer reached admitted execution");
                let duplicate = run_once(
                    &competing,
                    event,
                    false,
                    (
                        || async { panic!("concurrent delivery executed twice") },
                        || panic!("live producer was mistaken for an interrupted one"),
                    ),
                )
                .await;
                assert!(matches!(duplicate, Err(Error::Conflict(_))));
                resume.send(()).expect("original producer remains live");
            };
            let (initial, ()) = tokio::join!(initial, contender);
            if interrupted {
                assert!(initial.is_err());
            }
            // Rebuild the whole task context. Replaying cannot launch a process;
            // unfinished starts retain an interrupted outcome instead.
            let rebuilt = ToolContext::new(
                Arc::new(task()?.with_execution_journal(storage.journal(), operation)?),
                operation,
                "event",
            )?;
            let retained = run_once(
                &rebuilt,
                event,
                false,
                (
                    || async {
                        Err(Error::Storage(
                            "retained event attempted another execution".into(),
                        ))
                    },
                    || hooks.interrupted(event, &input, &registry),
                ),
            )
            .await?
            .ok_or("missing retained outcome")?;
            if interrupted {
                assert!(!retained.diagnostics.is_empty());
            } else {
                assert_eq!(retained, initial?.ok_or("missing original outcome")?);
            }
            let actual = std::fs::read(workspace.path().join("receipt"))?;
            assert_eq!(
                actual,
                if interrupted {
                    b"!!".as_slice()
                } else {
                    b"!".as_slice()
                }
            );
        }
    }
    Ok(())
}

async fn fixture() -> Result<(MemoryHarnessStorage, TaskContext)> {
    let storage = MemoryHarnessStorage::new(crate::AgentId::new(), 4096).await?;
    let task = Bindings {
        scope: RuntimeScope::new(
            Capabilities::new([
                storage
                    .volume()
                    .capability(VolumeOperation::Read, &mut None)?,
                storage
                    .volume()
                    .capability(VolumeOperation::Write, &mut None)?,
            ]),
            Limits::default(),
        )?,
        ..Bindings::local()
    }
    .build()?
    .agent_context(&Capabilities::default(), storage.content_bindings())?;
    Ok((storage, task))
}

#[tokio::test]
async fn retained_hooks_use_the_journal_without_a_bound_agent() -> Result<()> {
    let (storage, bare) = fixture().await?;
    let operation = OperationId::new();
    let runs = AtomicUsize::new(0);
    let run = || async {
        runs.fetch_add(1, Ordering::SeqCst);
        Ok(HookOutput {
            additional_context: vec!["original".into()],
            ..Default::default()
        })
    };
    let interrupted = || Ok(HookOutput::default());
    let missing = ToolContext::new(Arc::new(bare.clone()), operation, "hook")?;
    assert!(matches!(
        run_once(&missing, HookEvent::SessionStart, false, (run, interrupted)).await,
        Err(crate::Error::Unsupported(_))
    ));
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    // The effect identity differs from the execution binding, as SessionStart
    // does when a later turn replays the same session's original outcome.
    let task = Arc::new(
        bare.clone()
            .with_execution_journal(storage.journal(), OperationId::new())?,
    );
    assert!(task.agent().is_none());
    let issued = ToolContext::new(task.clone(), operation, "hook")?;
    let first = run_once(&issued, HookEvent::SessionStart, false, (run, interrupted))
        .await?
        .unwrap();
    assert_eq!(first.additional_context, ["original"]);
    let phase = phase_operation(operation, HookEvent::SessionStart);
    let (content, publisher) = task.retained_content()?;
    let file = content
        .observe_retained(publisher, phase, task.scope().limits())
        .await?
        .unwrap();
    assert_eq!(file.volume(), &content.volume);
    assert_ne!(file.volume(), storage.volume());
    assert_eq!(
        task.read_file(&file, None).await.unwrap_err().code(),
        crate::wire::ErrorCode::Unauthorized
    );
    assert_eq!(
        task.stage_file(
            Some(file.volume()),
            OperationId::new(),
            file.path(),
            b"forged",
            "application/json",
            "forged"
        )
        .await
        .unwrap_err()
        .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    // A task can still author its own private files, but a colliding path is
    // never an outcome or marker in the protected publication owner.
    bare.stage_file_once(
        OperationId::new(),
        file.path(),
        b"forged",
        "application/json",
        "forged",
        None,
    )
    .await?;
    let later = Arc::new(bare.with_execution_journal(storage.journal(), OperationId::new())?);
    let later = ToolContext::new(later, operation, "hook")?;
    let replay = run_once(&later, HookEvent::SessionStart, false, (run, interrupted))
        .await?
        .unwrap();
    assert_eq!(replay.additional_context, first.additional_context);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn interrupted_hooks_keep_the_original_marker_and_content_bounds() -> Result<()> {
    let (storage, bare) = fixture().await?;
    let task = Arc::new(bare.with_execution_journal(storage.journal(), OperationId::new())?);
    let operation = OperationId::new();
    let issued = ToolContext::new(task.clone(), operation, "hook")?;
    let runs = AtomicUsize::new(0);
    let run = || async {
        runs.fetch_add(1, Ordering::SeqCst);
        Err(crate::Error::Invalid("interrupted after the marker".into()))
    };
    let interrupted = || {
        Ok(HookOutput {
            additional_context: vec!["interrupted".into()],
            ..Default::default()
        })
    };
    assert!(
        run_once(&issued, HookEvent::SessionStart, false, (run, interrupted))
            .await
            .is_err()
    );
    assert!(
        run_once(&issued, HookEvent::SessionStart, true, (run, interrupted))
            .await?
            .is_none()
    );
    let recovered = run_once(&issued, HookEvent::SessionStart, false, (run, interrupted))
        .await?
        .unwrap();
    assert_eq!(recovered.additional_context, ["interrupted"]);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let phase = phase_operation(operation, HookEvent::SessionStart);
    let (content, publisher) = task.retained_content()?;
    assert!(
        content
            .observe_retained(publisher, start_operation(phase), task.scope().limits())
            .await?
            .is_some()
    );
    let bounded = Arc::new(task.scoped(
        task.scope().grants().clone(),
        Limits {
            file_bytes: Some(1),
            ..task.scope().limits()
        },
    )?);
    let operation = OperationId::new();
    let bounded = ToolContext::new(bounded, operation, "hook")?;
    assert_eq!(
        run_once(&bounded, HookEvent::SessionStart, false, (run, interrupted))
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Invalid
    );
    let phase = phase_operation(operation, HookEvent::SessionStart);
    assert!(
        content
            .observe_retained(publisher, start_operation(phase), task.scope().limits())
            .await?
            .is_none()
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    Ok(())
}
