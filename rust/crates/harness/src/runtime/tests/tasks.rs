use super::*;

#[test]
fn task_requirement_projection_rejects_missing_versions_and_cycles() {
        let tools = BTreeSet::from([(String::from("tool"), String::from("2"))]);
        let environment = TaskDependencyEnvironment::default();
        let grants = BTreeSet::new();
        let missing = dependency_fixture(&[("parent", &["task:leaf@1"])]);
        assert!(validate_task_requirements(&missing, &tools, &environment, &grants, None).is_err());

        let cycle = dependency_fixture(&[("left", &["task:right@1"]), ("right", &["task:left@1"])]);
        let result = validate_task_requirements(&cycle, &tools, &environment, &grants, None);
        assert!(result.is_err());
    }

#[test]
fn task_requirement_projection_checks_grants_and_extensions() {
        let tasks =
            dependency_fixture(&[("root", &["grant:task:run", "extension:example.state@3"])]);
        let mut environment = TaskDependencyEnvironment::default();
        environment
            .extensions
            .insert((String::from("example.state"), 3));
        let grants = BTreeSet::from([String::from("task:run")]);
        assert!(
            validate_task_requirements(&tasks, &BTreeSet::new(), &environment, &grants, None).is_ok()
        );
        environment.extensions.clear();
        assert!(
            validate_task_requirements(&tasks, &BTreeSet::new(), &environment, &grants, None).is_err()
        );
    }

#[tokio::test]
async fn live_task_model_override_is_scoped_and_durable_direct_call_is_rejected() -> Result<()>
    {
        let root_provider = Arc::new(CompletedModel {
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let child_provider = Arc::new(CompletedModel {
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
        let harness = AgentHarness::new(
            TaskRegistry::default(),
            ToolRegistry::default(),
            scope.clone(),
            2,
            None,
        )?
        .bind_model(
            Model::new("root".into(), "model".into(), "1".into(), Value::Null)?,
            root_provider.clone(),
        )?;
        let context = TaskContext {
            harness,
            task_id: OperationId::new(),
            durable_task: None,
            descendants: TaskGroup::new(2, None).unwrap(),
            scope,
            policy_overrides: Vec::new(),
            owned_interactions: None,
            owned_mailbox: None,
            output_content: None,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: std::sync::OnceLock::new(),
            model_steps: Arc::new((AtomicUsize::new(0), None)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
        };
        let message = ModelMessage {
            role: crate::model::ModelRole::User,
            content: crate::model::ModelContent::Text("hello".into()),
        };
        context
            .clone()
            .model_events(vec![message.clone()], None)
            .await?;
        let child = context.scoped_model(
            Capabilities::new(["model:generate"]),
            Limits::default(),
            Model::new("child".into(), "model".into(), "2".into(), Value::Null)?,
            child_provider.clone(),
        )?;
        child.model_events(vec![message.clone()], None).await?;
        let bounded = child.scoped_run_limits(TaskRunLimits {
            concurrency: None,
            max_steps: Some(2),
            deadline_epoch_ms: None,
        })?;
        assert!(bounded.model_events(vec![message], None).await.is_err());
        assert_eq!(
            root_provider
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())[0]
                .1
                .model
                .provider,
            "root"
        );
        assert_eq!(
            child_provider
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())[0]
                .1
                .model
                .provider,
            "child"
        );
        let root_dispatch = root_provider.requests.lock().unwrap()[0].0;
        let child_dispatch = child_provider.requests.lock().unwrap()[0].0;
        assert_eq!(root_dispatch.operation_id, context.task_id);
        assert_eq!(child_dispatch.operation_id, context.task_id);
        assert_eq!(root_dispatch.step, 0);
        assert_eq!(child_dispatch.step, 1);
        let foreign = FileRef::new(VolumeRef::new(ProviderRef::new("other".into(), "filesystem".into(), "2".into(), &mut None)?, ("private").into(), VolumeClass::AgentPrivate, VolumeOwner::Agent(crate::AgentId::from_bytes([77; 16])), &mut None)?, ("evidence/image.png").into(), ("generation-1").into(), FileDescriptor::from_bytes(b"image", "image/png", &mut None)?, ("image.png").into(), &mut None)?;
        assert!(matches!(
            context
                .model_events(
                    vec![ModelMessage {
                        role: crate::model::ModelRole::User,
                        content: ModelContent::Part(ModelContentPart::File {
                            file: foreign,
                            policy: crate::model::FileProjectionPolicy::Native(Box::new(
                                crate::model::NativeMediaPolicy {
                                    intent: crate::model::NativeMediaIntent::Image {
                                        detail: crate::model::ImageDetail::Auto
                                    },
                                    maximum_bytes: crate::conversation::MAX_EXACT_JS_INTEGER,
                                    maximum_work: 4096,
                                    configuration: None
                                }
                            )),
                        }),
                    }],
                    None
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(
            root_provider
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len(),
            1
        );
        let mut durable = child;
        durable.durable_task = Some(TaskId::new());
        assert!(matches!(
            durable
                .model_events(
                    vec![ModelMessage {
                        role: crate::model::ModelRole::User,
                        content: crate::model::ModelContent::Text("hello".into()),
                    }],
                    None
                )
                .await,
            Err(Error::Unsupported(_))
        ));
        let unregistered = Arc::new(TaskDefinition::live(
            "test.local.child",
            "1",
            |_, value: u32| async move { Ok(value) },
        )?);
        assert!(matches!(
            durable.group().spawn(&unregistered, 1).await,
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

#[test]
fn resumable_task_requires_pinned_nontrivial_input_and_output_schemas() -> Result<()> {
        let machine: Arc<dyn ResumableMachine> = Arc::new(TestMachine {
            identity: MachineIdentity {
                name: "test.machine".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            state_schema: serde_json::json!({"type": "object"}),
        });
        let input = serde_json::json!({"type": "object", "required": ["value"]});
        let output = serde_json::json!({"type": "integer"});
        assert!(
            TaskDefinition::<Value, u32>::resumable(
                machine.clone(),
                serde_json::json!({}),
                output.clone()
            )
            .is_err()
        );
        assert!(
            TaskDefinition::<Value, u32>::resumable(
                machine.clone(),
                input.clone(),
                Value::Bool(true)
            )
            .is_err()
        );
        let definition = TaskDefinition::<Value, u32>::resumable(machine, input, output)?;
        assert!(
            definition
                .schemas(
                    serde_json::json!({}),
                    serde_json::json!({"type": "integer"})
                )
                .is_err()
        );
        Ok(())
    }

#[tokio::test]
async fn local_stock_admission_coexists_with_a_durable_execution_route() -> Result<()> {
        struct RouteProbe {
            spawner: Arc<dyn TaskSpawner>,
            state: Arc<dyn TaskStateProvider>,
        }
        impl ExecutionProvider for RouteProbe {
            fn identity(&self) -> ComponentIdentity {
                ComponentIdentity {
                    name: "test.route".into(),
                    version: "1".into(),
                    digest: [1; 32],
                }
            }
            fn spawner(&self) -> Arc<dyn TaskSpawner> {
                Arc::clone(&self.spawner)
            }
            fn state(&self) -> Arc<dyn TaskStateProvider> {
                Arc::clone(&self.state)
            }
            fn qualify<'a>(
                &'a self,
                _: &'a TaskAdmissionRecord,
            ) -> BoxFuture<'a, Result<ExecutionPlacement>> {
                Box::pin(async { Err(Error::Conflict("local turn reached durable route".into())) })
            }
        }
        let record = TaskAdmissionRecord::from_canonical_value(
            serde_json::from_str(include_str!("../../../fixtures/v2/task-admission.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            &mut None,
        )?.0;
        let task_id = TaskId::from_bytes([4; 16]);
        let definition =
            TaskDefinition::live("test.local", "1", |context, input: u64| async move {
                Ok((context.task_id, input))
            })?;
        let mut bindings = Bindings::default();
        bindings.tasks.register(definition)?;
        let runtime = bindings.build()?;
        let definition = runtime.task::<u64, (OperationId, u64)>("test.local@1")?;
        let mut routed = runtime.as_ref().clone();
        routed.execution = Some(Arc::new(RouteProbe {
            spawner: Arc::new(SplitState {
                task_id,
                record: record.clone(),
            }),
            state: Arc::new(SplitState { task_id, record }),
        }));
        let routed = Arc::new(routed);
        let operation = OperationId::new();
        assert!(matches!(
            routed.spawn(&definition, 7).await,
            Err(Error::Unsupported(_))
        ));
        let RuntimeTask::Live(handle) = routed
            .spawn_local_with_operation(operation, &definition, 7)
            .await?
        else {
            return Err(Error::Conflict("local admission changed task kind".into()));
        };
        assert!(matches!(handle.result().await,
            Outcome::Succeeded(Ok((id, value))) if id == operation && value == 7));
        Ok(())
    }

#[tokio::test]
async fn independent_state_and_spawner_attest_the_full_request() -> Result<()> {
        let machine = Arc::new(TestMachine {
            identity: MachineIdentity {
                name: "test.split".into(),
                version: "1".into(),
                digest: [8; 32],
            },
            state_schema: serde_json::json!({"type": "object"}),
        });
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::<Value, u32>::resumable(
            machine.clone(),
            serde_json::json!({"type": "object"}),
            serde_json::json!({"type": "integer"}),
        )?)?;
        let definition = tasks.get::<Value, u32>("test.split")?;
        let scope = RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?;
        let operation_id = OperationId::from_bytes([9; 16]);
        let task_id = TaskId::from_bytes([10; 16]);
        let input = serde_json::json!({"value": 3});
        let record = TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id,
            task: definition.identity().clone(),
            machine: machine.identity().clone(),
            input: input.clone(),
            input_schema: definition.input_schema.clone(),
            output_schema: definition.output_schema.clone(),
            parent: None,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        };
        assert!(TaskAdmissionRecord::from_canonical_value(record.canonical_value(), &mut None)?.0 == record);
        let mut unknown = record.canonical_value();
        unknown
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("task admission object".into()))?
            .insert("legacy_content".into(), Value::Null);
        assert!(TaskAdmissionRecord::from_canonical_value(unknown, &mut None).is_err());
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope,
            concurrency: 1,
            durable_host: None,
            state: Some(Arc::new(SplitState {
                task_id,
                record: record.clone(),
            })),
            spawner: Some(Arc::new(SplitState {
                task_id,
                record: record.clone(),
            })),
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
        let admitted = harness
            .admit(operation_id, &definition, input, None)
            .await?;
        let Admission::Accepted(task) = admitted else {
            return Err(Error::Storage("split task was not admitted".into()));
        };
        assert_eq!(task.identity(), task_id.to_string());
        assert_eq!(task.result().await?, Outcome::Succeeded(7));
        assert_eq!(
            harness
                .reconcile_admission(operation_id)
                .await?
                .map(|value| value.0),
            Some(task_id)
        );
        assert!(matches!(
            harness
                .admit(
                    operation_id,
                    &definition,
                    serde_json::json!({"value": 4}),
                    None
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let mut changed_request = record.clone();
        changed_request.input = serde_json::json!({"value": 4});
        let mismatched_spawner = harness.bind_spawner(Arc::new(SplitState {
            task_id,
            record: changed_request,
        }))?;
        assert!(matches!(
            mismatched_spawner.reconcile_admission(operation_id).await,
            Err(Error::Conflict(_))
        ));
        let mut changed_schema = record;
        changed_schema.input_schema =
            serde_json::json!({"type": "object", "additionalProperties": true});
        assert!(matches!(
            harness.validate_observed_admission(&changed_schema, None).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

#[tokio::test]
async fn durable_reattachment_requires_the_exact_admitted_definition() -> Result<()> {
        let make = |name: &str, digest: [u8; 32]| -> Result<TaskDefinition<Value, u32>> {
            TaskDefinition::resumable(
                Arc::new(TestMachine {
                    identity: MachineIdentity {
                        name: name.into(),
                        version: "1".into(),
                        digest,
                    },
                    state_schema: serde_json::json!({"type": "object"}),
                }),
                serde_json::json!({"type": "object"}),
                serde_json::json!({"type": "integer"}),
            )
        };
        let mut tasks = TaskRegistry::default();
        tasks.register(make("test.first", [1; 32])?)?;
        tasks.register(make("test.second", [2; 32])?)?;
        let first = tasks.get::<Value, u32>("test.first")?;
        let operation_id = OperationId::from_bytes([5; 16]);
        let scope = RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?;
        let record = TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id,
            task: first.identity().clone(),
            machine: match &first.implementation {
                TaskImplementation::Resumable(machine) => machine.identity().clone(),
                TaskImplementation::Live(_) => unreachable!(),
            },
            input: serde_json::json!({}),
            input_schema: first.input_schema.clone(),
            output_schema: first.output_schema.clone(),
            parent: None,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        };
        let polls = Arc::new(AtomicUsize::new(0));
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope,
            concurrency: 1,
            durable_host: Some(Arc::new(IdentityHost {
                record,
                polls: Arc::clone(&polls),
            })),
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
        let second = harness.task::<Value, u32>("test.second")?;
        let task_id = TaskId::from_bytes([5; 16]);
        let recovered = harness
            .reconcile_admission(OperationId::from_bytes([5; 16]))
            .await?
            .ok_or_else(|| Error::NotFound("reconciled task".into()))?;
        assert_eq!(recovered.0, task_id);
        assert_eq!(&recovered.1, first.identity());
        assert!(matches!(
            harness.attach(task_id, &second).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            harness.attach(task_id, &first).await?.result().await?,
            Outcome::Succeeded(7)
        );
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        Ok(())
    }

#[test]
fn private_root_directory_grant_reads_descendants_without_write_authority() -> Result<()> {
        let volume = VolumeRef::new(ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?, ("private").into(), VolumeClass::AgentPrivate, VolumeOwner::Agent(crate::AgentId::from_bytes([77; 16])), &mut None)?;
        let file = FileRef::new(volume.clone(), ("notes/nested/message.txt").into(), ("version-1").into(), FileDescriptor::from_bytes(b"hello", "text/plain", &mut None)?, ("message.txt").into(), &mut None)?;
        let grants = Capabilities::new([volume.directory_read_capability("")?]);
        assert!(read_granted(&grants, &file)?);
        assert!(!grants.contains(&volume.capability(VolumeOperation::Write, &mut None)?));
        for capability in [
            file.read_capability()?,
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.directory_read_capability("notes")?,
            volume.directory_read_capability("notes/nested")?,
        ] {
            let grants = Capabilities::new([capability]);
            assert!(read_granted_with_work(&grants, &file, &mut None)?);
        }
        let unrelated = Capabilities::new([
            volume.directory_read_capability("notes/other")?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]);
        assert!(!read_granted_with_work(&unrelated, &file, &mut None)?);
        Ok(())
    }

#[test]
fn local_task_names_share_the_bounded_component_contract() -> Result<()> {
        TaskDefinition::live("leaf", "1", |_context, value: u32| async move { Ok(value) })?;
        assert!(
            TaskDefinition::live(
                "bad/name",
                "1",
                |_context, value: u32| async move { Ok(value) }
            )
            .is_err()
        );
        assert!(
            TaskDefinition::live("leaf", "bad version", |_context, value: u32| async move {
                Ok(value)
            })
            .is_err()
        );
        assert!(
            TaskDefinition::live("leaf@other", "1", |_context, value: u32| async move {
                Ok(value)
            })
            .is_err()
        );
        Ok(())
    }

#[tokio::test]
async fn pinned_task_versions_coexist_without_implicit_latest_selection() -> Result<()> {
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::live(
            "versioned",
            "1",
            |_, value: u32| async move { Ok(value + 1) },
        )?)?;
        tasks.register(TaskDefinition::live(
            "versioned",
            "2",
            |_, value: u32| async move { Ok(value + 2) },
        )?)?;
        assert!(matches!(
            tasks.get::<u32, u32>("versioned"),
            Err(Error::Conflict(_))
        ));
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 2,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
        for (version, expected) in [("1", 11), ("2", 12)] {
            let definition = harness.task::<u32, u32>(&format!("versioned@{version}"))?;
            assert_eq!(
                harness.spawn(&definition, 10).await?.result().await?,
                Outcome::Succeeded(expected)
            );
        }
        Ok(())
    }

#[test]
fn scoped_authority_never_widens_grants_or_limits() -> Result<()> {
        let root = RuntimeScope::new(
            Capabilities::new(["tool:call:test.echo"]),
            Limits::default(),
        )?;
        let mut narrowed = root.limits();
        narrowed.file_bytes /= 2;
        narrowed.render_bytes /= 2;
        let child = root.narrow(Capabilities::new([] as [String; 0]), narrowed)?;
        assert!(!child.grants().contains("tool:call:test.echo"));
        assert!(
            child
                .narrow(Capabilities::new(["tool:call:test.echo"]), narrowed)
                .is_err()
        );
        assert!(
            child
                .narrow(Capabilities::new([] as [String; 0]), root.limits())
                .is_err()
        );
        let harness =
            AgentHarness::new(TaskRegistry::default(), ToolRegistry::default(), root, 1, None)?;
        let view = harness.scoped(Capabilities::new([] as [String; 0]), narrowed)?;
        assert!(harness.scope.grants().contains("tool:call:test.echo"));
        assert!(!view.scope.grants().contains("tool:call:test.echo"));
        assert!(
            view.scoped(Capabilities::new(["tool:call:test.echo"]), narrowed)
                .is_err()
        );
        let bounded = harness.scoped_run_limits(TaskRunLimits {
            concurrency: Some(1),
            max_steps: Some(3),
            deadline_epoch_ms: Some(1_900_000_000_000),
        })?;
        assert_eq!(bounded.concurrency, 1);
        assert_eq!(bounded.scope.run_limits().max_steps, Some(3));
        assert!(
            bounded
                .scoped_run_limits(TaskRunLimits {
                    concurrency: Some(2),
                    max_steps: Some(3),
                    deadline_epoch_ms: Some(1_900_000_000_000),
                })
                .is_err()
        );
        assert!(
            bounded
                .scoped_run_limits(TaskRunLimits {
                    concurrency: Some(1),
                    max_steps: None,
                    deadline_epoch_ms: Some(1_900_000_000_000),
                })
                .is_err()
        );
        Ok(())
    }
