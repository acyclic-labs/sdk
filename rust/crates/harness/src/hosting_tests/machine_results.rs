//! Real local Machine execution observed through the ordinary served tree.
use super::*;
use crate::{
    Capabilities,
    agent::{AgentPath, AgentRecord, ConversationEntry},
    context::CompactionPolicy,
    conversation::Limits,
    executor::{ExecutionEvent, ExecutionJournal},
    filesystem::{
        FilesystemExecutionJournal, FilesystemJournalBinding, StockExecutors, Tasks,
        agent_authority,
    },
    model::{Model, ModelEvent, ToolCall},
    runtime::{Bindings, InProcessTargets, RuntimeScope},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use acyclic_machines::{
    Budgets, CompatibilityPolicy, DeviceKind, ExpirationPolicy, Image, ImageDigest, LifecycleClass,
    MachineContract, MachineId, MachineObservation, MachineState, Platform as MachinePlatform,
    SuspensionPolicy,
};
use acyclic_stream::{StreamProvider as _, wire as session_wire};
use futures::StreamExt as _;
use std::sync::Mutex;

/// The trusted native enrollment in this test binds each original forked
/// project selection to a separate directory and the public local provider.
/// This tests result provenance; it does not qualify a remote volume mount.
struct NativeTurns {
    issuer: AuthorityRoot,
    directories: Mutex<Vec<tempfile::TempDir>>,
}

impl TurnExecutors for NativeTurns {
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, crate::Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let volume = &agent
                .volumes
                .iter()
                .find(|binding| binding.name == "project")
                .ok_or_else(|| crate::Error::Invalid("missing selected project".into()))?
                .volume;
            let directory =
                tempfile::tempdir().map_err(|error| crate::Error::Storage(error.to_string()))?;
            std::fs::write(directory.path().join("selection"), volume.id())
                .map_err(|error| crate::Error::Storage(error.to_string()))?;
            let observation = MachineObservation {
                id: MachineId::new(),
                state: MachineState::Running,
                contract: MachineContract {
                    image: Image::Custom(
                        ImageDigest::new([1; 32]).map_err(crate::Error::Machines)?,
                    ),
                    selected_volume_ref: Some(volume.into()),
                    capabilities: Default::default(),
                    compatibility: CompatibilityPolicy::BestEffort,
                    compatibility_revision: [1; 32],
                    suspension: SuspensionPolicy::Manual,
                    expiration: ExpirationPolicy::Never,
                    network_policy_digest: [2; 32],
                    budgets: Budgets::default(),
                    platform: MachinePlatform::host(DeviceKind::Desktop)
                        .ok_or_else(|| crate::Error::Unsupported("no native platform".into()))?,
                    lifecycle: LifecycleClass::Provisioned,
                },
                endpoints: Vec::new(),
                last_checkpoint: None,
                created_at_unix_ms: 1,
                changed_at_unix_ms: 1,
            };
            let provider = acyclic_machines::local::provider(
                observation.clone(),
                acyclic_machines::local::LocalOptions {
                    cwd: directory.path().to_path_buf(),
                    environment: ["PATH", "PATHEXT", "SystemRoot"]
                        .into_iter()
                        .filter_map(|name| {
                            std::env::var(name).ok().map(|value| (name.into(), value))
                        })
                        .collect(),
                    process_custody: None,
                },
                Arc::new(MemoryStream::default()),
                acyclic_stream::StreamPath::new(format!("results/{}", observation.id))
                    .map_err(|error| crate::Error::Storage(error.to_string()))?,
            )
            .await
            .map_err(crate::Error::Machines)?;
            self.directories
                .lock()
                .map_err(|_| crate::Error::Storage("directory owner lost".into()))?
                .push(directory);
            let grants = Capabilities::new([
                crate::contract::capability::tool_call("shell"),
                volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                volume.capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            ]);
            let signed =
                self.issuer
                    .bootstrap_scope("selected-machine", &grants, None, &mut None)?;
            let workspace = crate::machines::MachinesHost::new(
                provider,
                ProviderRef::new("native".into(), "machines".into(), "1".into(), &mut None)?,
            )?
            .workspace(
                observation,
                volume,
                signed,
                self.issuer.verifier(&mut None)?,
            )?
            .with_shell(
                if cfg!(windows) {
                    "powershell.exe"
                } else {
                    "bash"
                }
                .into(),
                &crate::tools::shell::PLATFORM,
            )?;
            let limits = Limits {
                model_steps: 4,
                ..Default::default()
            };
            let bindings = Bindings {
                targets: Some(Arc::new(InProcessTargets::home(
                    InProcessTargets::local_record("selected-machine", "owner"),
                    Arc::new(workspace),
                )?)),
                scope: RuntimeScope::new(
                    Capabilities::new(
                        grants
                            .iter()
                            .map(str::to_owned)
                            .chain([crate::contract::capability::MODEL_GENERATE.to_owned()]),
                    ),
                    limits,
                )?,
                ..Default::default()
            }
            .build()?;
            let command = if cfg!(windows) {
                "[Console]::Write([IO.File]::ReadAllText('selection'))"
            } else {
                "cat selection"
            };
            let model = Arc::new(ScriptedModelProvider::new([
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        "shell-1".into(),
                        "shell".into(),
                        serde_json::json!({"command": command}),
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    }),
                ]),
                ScriptedTurn::Text("done".into()),
            ]));
            let tools =
                crate::preset::Preset::graphcoder().tools(|name| name == "shell", Vec::new())?;
            StockExecutors::new(
                (
                    Model::new(
                        "test".into(),
                        "text".into(),
                        "1".into(),
                        serde_json::Value::Null,
                    )?,
                    model,
                ),
                tools,
                (limits, CompactionPolicy::Disabled),
                Arc::new(Tasks(bindings)),
            )
            .executor(agent, turn, input, authority)
            .await
        })
    }
}

#[tokio::test]
async fn parent_and_fork_expose_original_machine_results_through_session_files() -> Result {
    use acyclic_harness_client::ReadTarget;
    let bound = binding_with(Arc::new(NativeTurns {
        issuer: AuthorityRoot::new(
            "machine-results",
            [47; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "machine-results".into(),
            },
        ),
        directories: Mutex::new(Vec::new()),
    }))?;
    let id = AgentId::from_bytes([31; 16]);
    let fresh = [
        (PRIVATE_VOLUME, VolumeClass::AgentPrivate),
        ("project", VolumeClass::Project),
    ]
    .into_iter()
    .map(|(name, class)| {
        Ok(VolumeBinding {
            name: name.into(),
            volume: VolumeRef::new(
                provider()?,
                format!("results-{name}"),
                class,
                VolumeOwner::Agent(crate::AgentId::from_bytes(id.to_bytes())),
                &mut None,
            )?,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    let Serving::Served(server) = serve(
        bound,
        Arc::new(Owner::default()),
        &IntentId::try_from("machine-results")?,
        RootRequest {
            agent: id,
            fresh,
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("root publication uncertain".into());
    };
    let client = Client::in_process_with_products(
        server.bound().agents().provider().clone(),
        acyclic_stream::session::testing::authority(),
        Some(Arc::new(server.bound().changes()?)),
        server.clone(),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let mut runner = {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .run(|actor, error| panic!("original actor {actor:?} failed: {error}"))
                .await
        })
    };
    let scenario = async {
        let root = server.root().clone();
        call(
            &client,
            "agents",
            &AgentCommand::Prompt {
                agent: root.clone(),
                id: IntentId::try_from("shell-parent")?,
                body: ModelContent::Text("shell".into()),
            },
        )
        .await??;
        let parent = observe(&client, &server, &root, 1).await?;
        let Ok(AgentReply::Spawned { agent: child, .. }) = call(
            &client,
            "agents",
            &AgentCommand::Spawn {
                parent: root,
                id: IntentId::try_from("fork-shell")?,
                brief: ModelContent::Text("shell".into()),
            },
        )
        .await?
        else {
            return Err("fork was not admitted".into());
        };
        let fork = observe(&client, &server, &child, 2).await?;
        assert_ne!(
            parent["execution"]["volume"]["id"],
            fork["execution"]["volume"]["id"]
        );
        assert_ne!(parent["execution"]["machine"], fork["execution"]["machine"]);
        // An exact result from a different agent is not readable under its peer's identity.
        let foreign: crate::conversation::FileRef =
            serde_json::from_value(parent["result_file"].clone())?;
        assert!(
            call(
                &client,
                "agents",
                &AgentCommand::Read {
                    agent: child.clone(),
                    target: ReadTarget::File {
                        file: foreign.clone()
                    },
                    offset: 0,
                }
            )
            .await?
            .is_err()
        );
        assert!(
            call(
                &client,
                "agents",
                &AgentCommand::Read {
                    agent: child,
                    target: ReadTarget::ToolOutput {
                        path: fork["result_file"]["path"]
                            .as_str()
                            .ok_or("result path missing")?
                            .into(),
                        at: Some(foreign),
                    },
                    offset: 0,
                }
            )
            .await?
            .is_err()
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let result = tokio::select! {
        result = scenario => result,
        stopped = &mut runner => return Err(format!("original Actor producer stopped: {stopped:?}").into()),
    };
    runner.abort();
    let _ = runner.await;
    result
}

async fn observe(
    client: &Client,
    server: &Arc<Server<Counted<MemoryStream>, LogicalObjectStore<acyclic_objects::MemoryObjects>>>,
    agent: &AgentPath,
    credential_id: u32,
) -> Result<serde_json::Value> {
    use acyclic_harness_client::ReadTarget;
    let bound = server.bound();
    let state = bound.agents().state(agent).await?;
    let authority = agent_authority(
        bound.issuer(),
        bound.admission(),
        agent,
        &state.volumes,
        &[],
    )
    .await?;
    let resource = bound.tool_resource(agent).await?;
    let credential = acyclic_auth::testing::root(|block| {
        use acyclic_auth::contract::*;
        let key = &mut block.certificates.as_mut().expect("chain").keys[0];
        key.key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: resource.clone(),
                        prefix: true,
                    },
                    actions: vec!["fs/list".into()],
                }),
            }],
        };
    });
    client.register_credential(credential_id, credential.into())?;
    let mut changes = client
        .subscribe(session_wire::SessionSubscription {
            credential: credential_id,
            resume: Some(session_wire::SessionCursor {
                next: 0,
                opaque: Default::default(),
            }),
            kind: Some(session_wire::session_subscription::Kind::Follow(
                session_wire::SessionFollow {
                    resource,
                    delivery: session_wire::SessionDelivery::All as i32,
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .await?;
    let mut request = acyclic_stream::FollowRequest::new(agent.conversation()?, 0);
    request.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut conversation =
        acyclic_stream::records_only(bound.agents().provider().follow(request).await?);
    let entry = loop {
        let record = conversation
            .next()
            .await
            .ok_or("conversation producer lost")??;
        let entry = ConversationEntry::decode(&record.value)?;
        if entry.output.volume() != &authority.private.volume {
            continue;
        }
        break entry;
    };
    // This scenario has one execution segment. Reopen its original journal
    // read-only and require its own ToolCompleted file, before awaiting delivery.
    let directory = entry
        .tool_results
        .as_deref()
        .ok_or("result directory missing")?;
    let operation: OperationId = serde_json::from_value(serde_json::json!(
        directory.rsplit('/').next().ok_or("missing turn")?
    ))?;
    let journal = FilesystemExecutionJournal::new(
        bound.agents().provider().clone(),
        bound.volumes().clone(),
        FilesystemJournalBinding {
            volume: &authority.private.volume,
            verifier: &authority.verifier,
            scope: &authority.scope,
            io: None,
        },
    )?
    .with_turn(operation, operation);
    let records = journal.replay(operation, 0, 128, None)?.await?;
    let file = records
        .into_iter()
        .find_map(|record| match record.event {
            ExecutionEvent::ToolCompleted {
                call_id, result, ..
            } if call_id == "shell-1" => Some(result),
            _ => None,
        })
        .ok_or("original completion missing")?;
    assert!(file.path().starts_with(&format!("{directory}/")));
    loop {
        let update = changes.next().await.ok_or("tool-output producer lost")??;
        if let Some(session_wire::session_frame::Body::Data(data)) = update.into_wire().body
            && let Some(session_wire::session_data::Item::Record(record)) = data.item
            && acyclic_stream::decode_changes(&record.value)?
                .entries
                .iter()
                .any(|entry| entry.key.as_ref() == file.path().as_bytes())
        {
            break;
        }
    }
    // A result read must bind the original immutable conversation cut, never
    // whatever bytes a later raw writer puts at the same path.
    let expected = journal.load(&file, None)?.await?;
    let workspace = crate::resources::WorkspaceRef::new(
        authority.private.volume.provider().clone(),
        authority
            .private
            .volume
            .storage_name(&mut None)?
            .into_bytes(),
        None,
    )?;
    bound
        .volumes()
        .apply(
            &workspace,
            None,
            &[crate::filesystem::WorkspaceMutation::PutFile {
                path: format!("/{}", file.path()),
                bytes: bytes::Bytes::from_static(b"{}").into(),
            }],
            &crate::IdempotencyKey::new(format!("replace-result-{credential_id}"))?,
        )
        .await?;
    assert!(
        call(
            client,
            "agents",
            &AgentCommand::Read {
                agent: agent.clone(),
                target: ReadTarget::ToolOutput {
                    path: file.path().into(),
                    at: None
                },
                offset: 0,
            }
        )
        .await?
        .is_err()
    );
    let reply = call(
        client,
        "agents",
        &AgentCommand::Read {
            agent: agent.clone(),
            target: ReadTarget::ToolOutput {
                path: file.path().into(),
                at: Some(entry.output.clone()),
            },
            offset: 0,
        },
    )
    .await??;
    let (bytes, _) = reply.content()?;
    assert_eq!(expected.as_ref(), bytes.as_slice());
    let mut result: serde_json::Value = serde_json::from_slice(&bytes)?;
    let mut value = result["value"].take();
    assert_eq!(value["execution"]["agent"], agent.as_str());
    assert_eq!(value["target"], "selected-machine");
    assert_eq!(value["execution"]["call_id"], "shell-1");
    assert_eq!(value["execution"]["turn"], serde_json::to_value(operation)?);
    assert!(!value["execution"]["operation"].is_null());
    let volume = &state
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or("project missing")?
        .volume;
    assert_eq!(value["execution"]["volume"], serde_json::to_value(volume)?);
    assert_eq!(value["stdout"], volume.id());
    value["result_file"] = serde_json::to_value(file)?;
    Ok(value)
}
