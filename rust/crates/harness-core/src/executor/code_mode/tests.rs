use super::*;
use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost, FilesystemJournalBinding,
};

#[cfg(feature = "code-mode")]
#[tokio::test]
async fn turn_cancellation_stops_shared_interpreter_without_closing_session() -> Result<()> {
    use crate::code_mode::{CodeControl, CodeHost, CodeHostResult};
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use std::task::{Context, Poll};

    struct Entered {
        original: NativeCodeControl,
        entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }
    impl CodeControl for Entered {
        fn check(&self) -> Result<()> {
            self.original.check()?;
            if let Some(entered) = self.entered.lock().unwrap().take() {
                entered
                    .send(())
                    .expect("cancellation producer remains live");
            }
            Ok(())
        }
        fn poll_interrupted(&self, context: &mut Context<'_>) -> Poll<Error> {
            self.original.poll_interrupted(context)
        }
        fn cancellation_token(&self) -> Option<&tokio_util::sync::CancellationToken> {
            self.original.cancellation_token()
        }
    }
    struct NoEffects;
    impl CodeHost for NoEffects {
        fn dispatch(
            &self,
            _: ToolInvocation,
        ) -> Result<crate::NativeFutureBundle<'_, CodeHostResult>> {
            panic!("the program cannot issue host effects")
        }
    }
    struct Held {
        work: NativeMaterializationWork,
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        result: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }
    impl CodeHost for Held {
        fn dispatch(
            &self,
            _: ToolInvocation,
        ) -> Result<crate::NativeFutureBundle<'_, CodeHostResult>> {
            let started = self.started.lock().unwrap().take().unwrap();
            let result = self.result.lock().unwrap().take().unwrap();
            crate::NativeFutureBundle::new(&self.work, || async move {
                started
                    .send(())
                    .expect("cancellation observer remains live");
                result.await.expect("the host producer remains live");
                panic!("cancellation retires this outstanding call")
            })
        }
    }

    let provider = crate::resources::ProviderRef::new(
        "code-stop".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = crate::AgentId::from_bytes([93; 16]);
    let volume = VolumeRef::new(
        provider,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "code-stop",
        [94; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "code-stop".into(),
        },
    );
    let grants = crate::Capabilities::new([
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let signed = issuer.bootstrap_scope("owner", &grants, Some(agent), &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
    let stop = acyclic_actors::guest::CancellationToken::new();
    let journal = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &verifier,
            scope: &signed,
            io: None,
        },
    )?
    .with_stop(stop.clone());
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 64 * 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let (owner, session) = crate::filesystem::SessionWork::admit(work.clone())?;
    let runtime = session.code_runtime()?;
    let io = session.preparation()?;
    let original = io.original_native_work().unwrap();
    let control = || -> Result<NativeCodeControl> {
        Ok(NativeCodeControl {
            work: original.clone(),
            cancelled: Mutex::new(original.cancellation_wait()?),
            stop: journal.model_stop().cloned(),
        })
    };
    // The synchronous interrupt observes the SAME delivery stop as the
    // journal's asynchronous wait, without cancelling the session ledger.
    stop.cancel();
    assert!(matches!(
        control()?.check(),
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        ))
    ));
    journal.model_cancellation().await?;
    original.remaining()?;

    let stop = acyclic_actors::guest::CancellationToken::new();
    let (entered, started) = std::sync::mpsc::channel();
    let control = Arc::new(Entered {
        original: NativeCodeControl {
            work: original.clone(),
            cancelled: Mutex::new(original.cancellation_wait()?),
            stop: Some(stop.clone()),
        },
        entered: Mutex::new(Some(entered)),
    });
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "loop".into(),
        name: "code".into(),
        arguments: serde_json::json!({}),
    };
    // A separate physical producer can interrupt synchronous JavaScript;
    // cancellation cannot depend on polling the blocked executor's task.
    let result = std::thread::scope(|scope| {
        let cancel = scope.spawn(move || {
            started.recv().expect("execution admission remains live");
            stop.cancel();
        });
        let result = futures::executor::block_on(runtime.execute(
            &invocation,
            "while (true) {}",
            &[],
            Arc::new(NoEffects),
            control,
        ));
        cancel.join().expect("cancellation producer completed");
        result
    });
    assert!(matches!(
        result,
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        ))
    ));
    assert!(Arc::ptr_eq(&runtime, &session.code_runtime()?));
    let stop = acyclic_actors::guest::CancellationToken::new();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (_producer, result) = tokio::sync::oneshot::channel();
    let host = Arc::new(Held {
        work: original.clone(),
        started: Mutex::new(Some(started)),
        result: Mutex::new(Some(result)),
    });
    let control = Arc::new(NativeCodeControl {
        work: original.clone(),
        cancelled: Mutex::new(original.cancellation_wait()?),
        stop: Some(stop.clone()),
    });
    let mut callable = code_tool()?.definition;
    callable.name = "held".into();
    let tools = [callable];
    let waiting = runtime.execute(
        &invocation,
        "return await held({code: 'pending'});",
        &tools,
        host,
        control,
    );
    let cancel = async {
        ready.await.expect("the original host call was admitted");
        stop.cancel();
    };
    let (waiting, ()) = tokio::join!(waiting, cancel);
    assert!(matches!(
        waiting,
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        ))
    ));
    let independent = Arc::new(NativeCodeControl {
        work: original.clone(),
        cancelled: Mutex::new(original.cancellation_wait()?),
        stop: None,
    });
    assert_eq!(
        runtime
            .execute(
                &invocation,
                "return 42;",
                &[],
                Arc::new(NoEffects),
                independent,
            )
            .await?
            .value,
        serde_json::json!(42)
    );
    drop((io, owner, runtime, session));
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn nested_files_use_original_journal_startup_and_replay_without_republication() -> Result<()>
{
    let provider = crate::resources::ProviderRef::new(
        "nested-files".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = crate::AgentId::from_bytes([91; 16]);
    let volume = VolumeRef::new(
        provider,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "nested-files",
        [92; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "nested-files".into(),
        },
    );
    let grants = crate::Capabilities::new([
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
        "tool:call:read".into(),
        "tool:call:write".into(),
    ]);
    let signed = issuer.bootstrap_scope("owner", &grants, Some(agent), &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
    let journal = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host.clone(),
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &verifier,
            scope: &signed,
            io: None,
        },
    )?;
    let scope = crate::runtime::RuntimeScope::new(grants, Limits::default())?;
    let content = crate::runtime::ContentBindings {
        volume: volume.clone(),
        reader: FilesystemContentVerifier::new(host.clone(), &verifier, &signed, 65_536, None)?,
        writer: Some(FilesystemContentPublisher::new(
            host, &volume, &verifier, &signed, 65_536, None,
        )?),
    };
    let task = Arc::new(
        crate::runtime::Bindings {
            scope: scope.clone(),
            content: Some(content.clone()),
            ..crate::runtime::Bindings::local()
        }
        .build()?
        .agent_context(&crate::Capabilities::default(), content)?,
    );
    let stock = StockExecutor::new(
        Model::new("test".into(), "model".into(), "1".into())?,
        Arc::new(crate::test_support::ScriptedModelProvider::new(Vec::new())),
        ContextPipeline::default(),
        crate::tools::defaults()?,
    )
    .with_compaction_policy(crate::context::CompactionPolicy::Disabled)
    .with_tool_authority(
        scope,
        Arc::new(crate::test_support::FixedToolPolicy::new(
            ToolPolicyDecision::Allow,
        )?),
    )?;
    let mut execution = StockExecution::ordinary(&stock);
    execution.context = Some((&task, OperationId::new()));
    let write = ToolInvocation::for_model_call(
        OperationId::new(),
        0,
        "write".into(),
        "write".into(),
        serde_json::json!({"path":"note","content":"first"}),
        None,
    )?;
    let metadata = serde_json::json!({"revision":"original"});
    let result = execution
        .nested_dispatch(&journal, write.clone(), &metadata, false)
        .await?;
    let ProfileDispatch::Settled(first, _) = result else {
        panic!("nested write did not settle")
    };
    let (_, before) = execution
        .step_records(&journal, write.operation_id, 0, true, 8, |_| true)
        .await?;
    assert!(matches!(
        before.first().map(|record| &record.event),
        Some(ExecutionEvent::Started { .. })
    ));
    assert!(
        before
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
    );
    let replay = execution
        .nested_dispatch(&journal, write.clone(), &metadata, false)
        .await?;
    let ProfileDispatch::Settled(replayed, _) = replay else {
        panic!("nested replay did not settle")
    };
    assert_eq!(first, replayed);
    let (_, after) = execution
        .step_records(&journal, write.operation_id, 0, true, 8, |_| true)
        .await?;
    assert_eq!(
        before, after,
        "replay must not publish another start or effect"
    );
    let mut changed_arguments = write.clone();
    changed_arguments.arguments["content"] = serde_json::json!("changed");
    for (changed, changed_metadata) in [
        (write.clone(), serde_json::json!({"revision":"changed"})),
        (changed_arguments, metadata.clone()),
    ] {
        let error = match execution
            .nested_dispatch(&journal, changed, &changed_metadata, false)
            .await
        {
            Ok(_) => panic!("changed nested request reused an execution"),
            Err(error) => error,
        };
        assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
    }
    let read = ToolInvocation::for_model_call(
        OperationId::new(),
        0,
        "read".into(),
        "read".into(),
        serde_json::json!({"path":"note"}),
        None,
    )?;
    let ProfileDispatch::Settled(read, _) = execution
        .nested_dispatch(&journal, read, &metadata, false)
        .await?
    else {
        panic!("nested read did not settle")
    };
    assert_eq!(read.value["text"], "first");
    Ok(())
}
