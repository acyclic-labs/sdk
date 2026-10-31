use super::*;
use crate::executor::records::RecordedCall;
use crate::runtime::{Target, TargetResolver};

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn filesystem_calls_record_the_provider_binding_and_refuse_retargeted_recovery()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use crate::{
        agent::{AgentPath, AgentState, ForkAdopted, VolumeBinding},
        conversation::{VolumeClass, VolumeOperation, VolumeOwner},
        core::{AggregateKind, Authority, AuthorityRoot},
        filesystem::{
            FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost, FilesystemTools,
            TurnAuthority,
        },
        runtime::{Bindings, ContentBindings, FilesystemExecutor},
    };

    let provider = ProviderRef::new(
        "bound-filesystem".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let filesystem = acyclic_fs::Fs::memory();
    let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    let owner = AgentId::from_bytes([71; 16]);
    let volume = |id: &str| {
        VolumeRef::new(
            provider.clone(),
            id.into(),
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
            &mut None,
        )
    };
    let first = volume("original")?;
    let second = volume("replacement")?;
    host.create_volume(&first).await?;
    host.create_volume(&second).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let root = filesystem
        .open_workspace(
            first.storage_name(&mut None)?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let store = acyclic_fs::StreamGitCompatStore::new(acyclic_fs::StreamAuthorityStore::new(
        filesystem.stream_provider(),
    ));
    let graph = acyclic_fs::WorkspaceGraph::new(store.clone());
    graph.register_root(&root).await?;
    let child = graph
        .fork(&root, "saved-source", acyclic_fs::IdempotencyKey::new())
        .await?;
    let repository = acyclic_fs::GitCompatRepository::new(root.id(), store);
    repository
        .register_branch_workspace("saved-source", child.id(), None, false)
        .await?;
    let mut transaction = root
        .begin_transaction(
            acyclic_fs::IdempotencyKey::new(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    transaction
        .write(
            "/secret",
            bytes::Bytes::from_static(b"saved original contents"),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    transaction
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    let issuer = AuthorityRoot::new(
        "bound-filesystem",
        [72; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "bound-filesystem".into(),
        },
    );
    let mut labels = vec!["tool:call:fs".to_owned()];
    for volume in [&first, &second] {
        for action in [VolumeOperation::Read, VolumeOperation::Write] {
            labels.push(volume.capability(action, &mut None)?);
        }
    }
    let grants = Capabilities::new(labels);
    let scope = issuer.bootstrap_scope("owner", &grants, Some(owner), &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
    let content = ContentBindings {
        volume: first.clone(),
        reader: FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 1_000_000, None)?,
        writer: Some(FilesystemContentPublisher::new(
            host.clone(),
            &first,
            &verifier,
            &scope,
            1_000_000,
            None,
        )?),
    };
    let journal = Arc::new(Journal::default());
    let authority = TurnAuthority {
        issuer: issuer.into(),
        scope,
        grants: grants.clone(),
        content: content.clone(),
        approval: allowed()?,
        effects: None,
        journal: journal.clone(),
        children: None,
        workspace_tools: None,
    };
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([71; 16]));
    let agent = |volume: &VolumeRef| {
        AgentState::adopted(
            path.clone(),
            &ForkAdopted {
                agent: path.id().clone(),
                parent: None,
                forked_at: 0,
                conversation_tail: 0,
                checkpoint: None,
                volumes: vec![VolumeBinding {
                    name: "project".into(),
                    volume: volume.clone(),
                }],
                brief: None,
                policy: None,
            },
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    };
    let original: Arc<dyn FilesystemExecutor> = Arc::new(FilesystemTools::new(
        host.clone(),
        &agent(&first)?,
        &authority,
    )?);
    let replacement: Arc<dyn FilesystemExecutor> =
        Arc::new(FilesystemTools::new(host, &agent(&second)?, &authority)?);
    assert_ne!(original.binding_identity(), replacement.binding_identity());
    let foreign_provider = ProviderRef::new(
        "another-filesystem".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let foreign: Arc<dyn FilesystemExecutor> = Arc::new(FilesystemTools::new(
        FilesystemHost::new(acyclic_fs::Fs::memory(), &foreign_provider, None)?,
        &agent(&first)?,
        &authority,
    )?);
    assert_ne!(original.binding_identity(), foreign.binding_identity());
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
    let bind = |filesystem| {
        Bindings {
            filesystem,
            scope: runtime_scope.clone(),
            ..Bindings::local()
        }
        .build()?
        .agent_context(&Capabilities::default(), content.clone())
        .map(|task| task.with_execution_journal(journal.clone()))
    };
    let base = uncompacted_executor(
        Model::new("fixture".into(), "bound-filesystem".into(), "1".into())?,
        Arc::new(scripted_model()),
        ContextPipeline::default(),
        crate::tools::defaults()?,
    )
    .with_tool_authority(runtime_scope.clone(), allowed()?)?;
    let commit_operation = OperationId::new();
    let commit = ToolInvocation {
        operation_id: commit_operation,
        call_id: "commit".into(),
        name: "fs".into(),
        arguments: json!({"command":"commit -m base"}),
    };
    let committing = base
        .clone()
        .with_task_context(&bind(Some(original.clone()))?, commit_operation)?;
    StockExecution::ordinary(&committing)
        .settle_tool_call(
            &*journal,
            commit_operation,
            0,
            commit.clone(),
            0,
            &mut vec![call_message(&commit)],
            &mut Vec::new(),
        )
        .await?;
    let operation = OperationId::new();
    let invocation = ToolInvocation {
        operation_id: operation,
        call_id: "status".into(),
        name: "fs".into(),
        arguments: json!({"command":"show HEAD:secret"}),
    };
    let bound = base
        .clone()
        .with_task_context(&bind(Some(original.clone()))?, operation)?;
    let mut messages = vec![call_message(&invocation)];
    StockExecution::ordinary(&bound)
        .settle_tool_call(
            &*journal,
            operation,
            0,
            invocation.clone(),
            0,
            &mut messages,
            &mut Vec::new(),
        )
        .await?;
    assert!(serde_json::to_string(&messages)?.contains("saved original contents"));
    let (_, records) = StockExecution::ordinary(&bound)
        .tool_records(&*journal, operation, 0, "status")
        .await?;
    assert!(
        matches!(
            RecordedCall::classify(&records).expect("valid claim"),
            RecordedCall::Completed { .. }
        ),
        "real filesystem status did not complete: {records:?}"
    );
    let reference = RecordedCall::classify(&records)
        .expect("valid claim")
        .started()
        .expect("started");
    let saved = load_json::<crate::tool::RecordedInvocation>(&*journal, reference, None).await?;
    assert_eq!(saved.0.target, None);
    assert_eq!(saved.0.filesystem, Some(original.binding_identity()));
    for filesystem in [Some(replacement.clone()), None, Some(foreign.clone())] {
        let changed = base
            .clone()
            .with_task_context(&bind(filesystem)?, operation)?;
        let error = StockExecution::ordinary(&changed)
            .settle_tool_call(
                &*journal,
                operation,
                0,
                invocation.clone(),
                0,
                &mut vec![call_message(&invocation)],
                &mut Vec::new(),
            )
            .await
            .err();
        assert!(
            matches!(&error, Some(Error::Indeterminate(id)) if *id == operation),
            "changed binding returned {error:?}"
        );
    }
    assert!(matches!(
        StockExecution::ordinary(&bound)
            .settle_tool_call(
                &*journal,
                operation,
                0,
                invocation.clone(),
                0,
                &mut vec![call_message(&invocation)],
                &mut Vec::new(),
            )
            .await?,
        ExecutionProgress::Ready(())
    ));
    let (_, records) = StockExecution::ordinary(&bound)
        .tool_records(&*journal, operation, 0, "status")
        .await?;
    assert!(matches!(
        RecordedCall::classify(&records).expect("valid claim"),
        RecordedCall::Completed { .. }
    ));

    let saved_state = repository.load().await?;
    let mut policy: acyclic_fs::access::Policy =
        serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
    for workspace in [&child, &root] {
        policy.version += 1;
        policy.layers[0][0].effect = acyclic_fs::access::Effect::Deny;
        replace_replay_policy(workspace, &policy).await?;
        let mut replay = vec![call_message(&invocation)];
        let refused = StockExecution::ordinary(&bound)
            .settle_tool_call(
                &*journal,
                operation,
                0,
                invocation.clone(),
                0,
                &mut replay,
                &mut Vec::new(),
            )
            .await;
        assert!(refused.is_err(), "revoked source exposed saved output");
        assert_eq!(replay.len(), 1, "denied output reached the model");
        policy.version += 1;
        policy.layers[0][0].effect = acyclic_fs::access::Effect::Allow;
        replace_replay_policy(workspace, &policy).await?;
        let mut replay = vec![call_message(&invocation)];
        assert!(matches!(
            StockExecution::ordinary(&bound)
                .settle_tool_call(
                    &*journal,
                    operation,
                    0,
                    invocation.clone(),
                    0,
                    &mut replay,
                    &mut Vec::new(),
                )
                .await?,
            ExecutionProgress::Ready(())
        ));
        assert_eq!(replay, messages, "replay changed the saved contents");
        assert_eq!(
            repository.load().await?,
            saved_state,
            "replay ran a command"
        );
        let (_, replay_records) = StockExecution::ordinary(&bound)
            .tool_records(&*journal, operation, 0, "status")
            .await?;
        assert_eq!(replay_records, records, "replay changed terminal records");
    }

    // A committed start without its terminal reply cannot select a different
    // current volume, or gain redispatch permission when its binding is lost.
    let operation = OperationId::new();
    let invocation = ToolInvocation {
        operation_id: operation,
        ..invocation
    };
    let start = stage_json(
        &*journal,
        operation,
        "invocation",
        &crate::tool::RecordedInvocation {
            invocation: &invocation,
            metadata: &Value::Null,
            target: None,
            filesystem: Some(original.binding_identity()),
        },
        None,
    )
    .await?;
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::ToolStarted {
                step: 0,
                call_id: invocation.call_id.clone(),
                invocation: start,
            },
            None,
        )?
        .await?;
    for filesystem in [Some(replacement), None, Some(foreign)] {
        let bound = base
            .clone()
            .with_task_context(&bind(filesystem)?, operation)?;
        let error = StockExecution::ordinary(&bound)
            .settle_tool_call(
                &*journal,
                operation,
                0,
                invocation.clone(),
                0,
                &mut vec![call_message(&invocation)],
                &mut Vec::new(),
            )
            .await
            .err();
        assert!(
            matches!(&error, Some(Error::Indeterminate(id)) if *id == operation),
            "changed binding returned {error:?}"
        );
        let (_, records) = StockExecution::ordinary(&bound)
            .tool_records(&*journal, operation, 0, "status")
            .await?;
        assert!(matches!(
            RecordedCall::classify(&records).expect("valid claim"),
            RecordedCall::Started(_)
        ));
    }
    let bound = base.with_task_context(&bind(Some(original))?, operation)?;
    let recovery = bound.clone().into_reconciliation();
    let error = StockExecution::ordinary(&recovery)
        .settle_tool_call(
            &*journal,
            operation,
            0,
            invocation.clone(),
            0,
            &mut vec![call_message(&invocation)],
            &mut Vec::new(),
        )
        .await
        .err();
    assert!(
        matches!(&error, Some(Error::Indeterminate(id)) if *id == operation),
        "original reconciliation returned {error:?}"
    );
    let (_, records) = StockExecution::ordinary(&bound)
        .tool_records(&*journal, operation, 0, "status")
        .await?;
    assert!(matches!(
        RecordedCall::classify(&records).expect("valid claim"),
        RecordedCall::Started(_)
    ));
    // Ordinary recovery keeps the existing terminal interruption policy; a
    // matching binding never turns the unanswered start into a fresh dispatch.
    assert!(matches!(
        StockExecution::ordinary(&bound)
            .settle_tool_call(
                &*journal,
                operation,
                0,
                invocation.clone(),
                0,
                &mut vec![call_message(&invocation)],
                &mut Vec::new(),
            )
            .await?,
        ExecutionProgress::Failed(ToolFailureKind::Interrupted)
    ));
    let (_, records) = StockExecution::ordinary(&bound)
        .tool_records(&*journal, operation, 0, "status")
        .await?;
    assert!(matches!(
        RecordedCall::classify(&records).expect("valid claim"),
        RecordedCall::Failed {
            started: Some(_),
            reason: ToolFailureKind::Interrupted,
            ..
        }
    ));
    Ok(())
}

#[cfg(feature = "filesystem")]
async fn replace_replay_policy<
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
>(
    workspace: &acyclic_fs::Workspace<A, O>,
    policy: &acyclic_fs::access::Policy,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(policy)?,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let outcome = checkout
        .commit::<acyclic_fs::FsError, _, _>(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                })
            },
        )
        .await?
        .value;
    assert!(matches!(
        outcome,
        acyclic_fs::LivePublicationObservation::Committed { .. }
    ));
    Ok(())
}

struct Devices {
    roster: Arc<crate::agent::Roster>,
    devices: Vec<Arc<Target>>,
}

impl TargetResolver for Devices {
    fn roster(&self) -> &Arc<crate::agent::Roster> {
        &self.roster
    }

    fn resolve(&self, name: Option<&str>) -> Result<Arc<Target>> {
        let selected = self.roster.select(name).map_err(Error::Target)?;
        self.devices
            .iter()
            .find(|device| device.record.name == selected.name)
            .cloned()
            .ok_or_else(|| Error::Invalid("selected device is absent".into()))
    }
}

// Exercise the durable stock claim and real workspace router, with a lost
// reply at the device boundary. Changing a roster cannot change that claim.
#[tokio::test]
async fn started_workspace_call_keeps_its_device_after_home_changes() -> Result<()> {
    for (name, alias) in [("shell", None), ("shell", Some("user:ana")), ("fs", None)] {
        for absent in [false, true] {
            let scope = RuntimeScope::new(
                Capabilities::new([crate::contract::capability::tool_call(name)]),
                Limits::default(),
            )?;
            let task = admitted_context(scope.clone())?;
            let original = Arc::new(ContextOnlyTool {
                admission: context_identity(&task).cloned(),
                lost_response: || Error::Storage("device reply lost after effect".into()),
                calls: AtomicUsize::new(0),
                reconciles: AtomicUsize::new(0),
            });
            let other = Arc::new(ContextOnlyTool {
                admission: context_identity(&task).cloned(),
                lost_response: || Error::Storage("unrelated device".into()),
                calls: AtomicUsize::new(0),
                reconciles: AtomicUsize::new(0),
            });
            let device = |name, executor: Arc<dyn ToolExecutor>| {
                Arc::new(Target {
                    record: crate::runtime::InProcessTargets::local_record(name, "ana"),
                    executor,
                })
            };
            let old = device("original", original.clone());
            let new = device("replacement", other.clone());
            let bind = |devices: Vec<Arc<Target>>| -> Result<crate::runtime::TaskContext> {
                let mut task = task.clone();
                task.harness = crate::runtime::Bindings {
                    scope: scope.clone(),
                    targets: Some(Arc::new(Devices {
                        roster: Arc::new(
                            crate::agent::Roster::new(
                                &devices[0].record.name,
                                devices.iter().map(|device| device.record.clone()).collect(),
                            )
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                        ),
                        devices,
                    })),
                    ..crate::runtime::Bindings::local()
                }
                .build()?;
                Ok(task)
            };
            let defaults = crate::tools::defaults()?;
            let mut tools = ToolRegistry::default();
            tools.register(
                (
                    crate::tool::Tool {
                        hook_names: &[],
                        definition: crate::tool::ToolDefinition {
                            name: name.into(),
                            revision: "1".into(),
                            description: "Workspace command".into(),
                            input_schema: json!({"type":"object"}),
                            output_schema: json!({"type":"object"}),
                            projection_schema: crate::tool::json_projection_schema(
                                json!({"type":"object"}),
                            ),
                        },
                        executor: defaults
                            .get(name, None, None)?
                            .expect("stock workspace command")
                            .executor
                            .clone(),
                        projection: Arc::new(Projection),
                    },
                    None,
                ),
                &mut None,
                None,
            )?;
            let executor = uncompacted_executor(
                Model::new("test".into(), "replay".into(), "1".into())?,
                Arc::new(scripted_model()),
                ContextPipeline::default(),
                tools,
            )
            .with_tool_authority(scope.clone(), allowed()?)?;
            let operation = OperationId::new();
            let mut invocation = ToolInvocation {
                operation_id: operation,
                call_id: "command".into(),
                name: name.into(),
                arguments: json!({"command":if name == "fs" { "status" } else { "echo retained" }}),
            };
            if let Some(alias) = alias {
                invocation.arguments["target"] = alias.into();
            }
            let journal = Journal::default();
            let first = executor
                .clone()
                .with_task_context(&bind(vec![old.clone(), new.clone()])?, operation)?;
            let mut messages = vec![call_message(&invocation)];
            assert!(matches!(
                StockExecution::ordinary(&first)
                    .settle_tool_call(
                        &journal,
                        operation,
                        0,
                        invocation.clone(),
                        0,
                        &mut messages,
                        &mut Vec::new(),
                    )
                    .await,
                Err(Error::Indeterminate(_))
            ));
            let (_, records) = StockExecution::ordinary(&first)
                .tool_records(&journal, operation, 0, "command")
                .await?;
            let reference = RecordedCall::classify(&records)
                .expect("valid claim")
                .started()
                .expect("started");
            let saved =
                load_json::<crate::tool::RecordedInvocation>(&journal, reference, None).await?;
            assert_eq!(saved.0.target.as_deref(), Some("original"));
            assert_eq!(
                saved.0.invocation, invocation,
                "model arguments stay unchanged"
            );

            let mut changed = vec![new.clone()];
            if !absent {
                changed.push(old.clone());
            }
            let second = executor
                .clone()
                .with_task_context(&bind(changed)?, operation)?;
            let replay = StockExecution::ordinary(&second)
                .settle_tool_call(
                    &journal,
                    operation,
                    0,
                    invocation.clone(),
                    0,
                    &mut messages,
                    &mut Vec::new(),
                )
                .await;
            if absent {
                assert!(matches!(replay, Err(Error::Indeterminate(id)) if id == operation));
                assert_eq!(original.reconciles.load(Ordering::SeqCst), 0);
                // Restore the original device; only its original effect may settle.
                let restored = executor
                    .clone()
                    .with_task_context(&bind(vec![new, old])?, operation)?;
                StockExecution::ordinary(&restored)
                    .settle_tool_call(
                        &journal,
                        operation,
                        0,
                        invocation,
                        0,
                        &mut messages,
                        &mut Vec::new(),
                    )
                    .await?;
            } else {
                replay?;
            }
            assert_eq!(original.calls.load(Ordering::SeqCst), 1);
            assert_eq!(original.reconciles.load(Ordering::SeqCst), 1);
            assert_eq!(other.calls.load(Ordering::SeqCst), 0);
            assert_eq!(other.reconciles.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}
