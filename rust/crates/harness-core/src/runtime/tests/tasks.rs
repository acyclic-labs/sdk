use super::*;

#[tokio::test]
async fn indeterminate_live_join_keeps_command_causes() -> Result<()> {
    for reason in [
        crate::CommandIndeterminacyReason::Failed {
            code: "ERROR_CODE_INVALID".into(),
            error: "invalid model result".into(),
        },
        crate::CommandIndeterminacyReason::Unsettled,
        crate::CommandIndeterminacyReason::InputLimit,
    ] {
        let cause = crate::IndeterminateCause::Command(crate::CommandIndeterminacy {
            command: "acyclic.model.v1".into(),
            reason,
        });
        let expected = cause.clone();
        let definition = TaskDefinition::live("test.uncertain", "1", move |context, (): ()| {
            let cause = cause.clone();
            async move { Err::<(), _>(cause.into_error(context.task_id)) }
        })?;
        let mut bindings = Bindings::local();
        bindings.tasks.register(definition)?;
        let runtime = bindings.build(None)?;
        let definition = runtime.task::<(), ()>("test.uncertain@1")?;
        let operation_id = OperationId::new();
        let task = runtime
            .spawn_local_with_operation(operation_id, &definition, ())
            .await?;
        assert_eq!(
            task.result().await?,
            Outcome::Indeterminate {
                operation_id,
                cause: expected
            }
        );
    }
    Ok(())
}

#[test]
fn task_requirements_reject_missing_versions_and_cycles() -> Result<()> {
    assert!(
        dependency_fixture(&[("parent", &["task:leaf@1"])])?
            .build(None)
            .is_err()
    );
    assert!(
        dependency_fixture(&[("left", &["task:right@1"]), ("right", &["task:left@1"])])?
            .build(None)
            .is_err()
    );
    Ok(())
}

#[test]
fn task_requirements_check_bound_grants_and_extensions() -> Result<()> {
    let mut bindings = dependency_fixture(&[("root", &["grant:task:run"])])?;
    bindings.scope = RuntimeScope::new(Capabilities::new(["task:run"]), Limits::default())?;
    assert!(bindings.build(None).is_ok());
    assert!(
        dependency_fixture(&[("root", &["grant:task:run"])])?
            .build(None)
            .is_err()
    );
    let mut bindings =
        dependency_fixture(&[("root", &["grant:task:run", "extension:example.state@3"])])?;
    bindings.scope = RuntimeScope::new(Capabilities::new(["task:run"]), Limits::default())?;
    assert!(bindings.build(None).is_err());
    Ok(())
}

#[tokio::test]
async fn live_model_checks_file_authority_and_rejects_durable_direct_calls() -> Result<()> {
    let root_provider = Arc::new(
        crate::test_support::ScriptedModelProvider::new([
            crate::test_support::ScriptedTurn::Events(vec![
                crate::test_support::ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
        ])
        .with_token_counter(|request| {
            Ok(crate::context::ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                u32::try_from(request.bytes().len())
                    .map_err(|_| Error::Invalid("fixture token count overflow".into()))?,
                vec![0; request.request().messages.len()],
            ))
        }),
    );
    let scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
    let harness = crate::runtime::Bindings {
        scope: scope.clone(),
        concurrency: 2,
        ..crate::runtime::Bindings::local()
    }
    .build(None)?
    .bind_model(
        Model::new("root".into(), "model".into(), "1".into(), Value::Null)?,
        root_provider.clone(),
    )?;
    let context = test_context(harness)?;
    let message = ModelMessage {
        role: crate::model::ModelRole::User,
        content: crate::model::ModelContent::Text("hello".into()),
    };
    context
        .clone()
        .model_events(vec![message.clone()], None)
        .await?;
    let foreign = file_fixture("evidence/image.png", "image/png")?;
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
    assert_eq!(root_provider.requests().len(), 1);
    let mut durable = context;
    durable.durable_task = Some(TaskId::new());
    assert!(matches!(
        durable.model_events(vec![message], None).await,
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
    let original = resumable_fixture("test.machine", [1; 32])?;
    let TaskImplementation::Resumable(machine) = original.implementation else {
        unreachable!()
    };
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
        TaskDefinition::<Value, u32>::resumable(machine.clone(), input.clone(), Value::Bool(true))
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
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
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
        async fn qualify(&self, _: &TaskAdmissionRecord) -> Result<ExecutionPlacement> {
            Err(Error::Conflict("local turn reached durable route".into()))
        }
    }
    let record = admission_fixture()?;
    let task_id = TaskId::from_bytes([4; 16]);
    let definition = TaskDefinition::live("test.local", "1", |context, input: u64| async move {
        Ok((context.task_id, input))
    })?;
    let mut bindings = Bindings::default();
    bindings.tasks.register(definition)?;
    let runtime = bindings.build(None)?;
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
    let mut tasks = TaskRegistry::default();
    tasks.register(resumable_fixture("test.split", [8; 32])?)?;
    let definition = tasks.get::<Value, u32>("test.split")?;
    let scope = RuntimeScope::default();
    let operation_id = OperationId::from_bytes([9; 16]);
    let task_id = TaskId::from_bytes([10; 16]);
    let input = serde_json::json!({"value": 3});
    let record = tasks.registered_admission(
        operation_id,
        "test.split",
        "1",
        input.clone(),
        None,
        None,
        &scope,
        &mut None,
    )?;
    let harness = Bindings {
        tasks,
        scope,
        concurrency: 1,
        state: Some(Arc::new(SplitState {
            task_id,
            record: record.clone(),
        })),
        spawner: Some(Arc::new(SplitState {
            task_id,
            record: record.clone(),
        })),
        ..Bindings::local()
    }
    .build(None)?;
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
    let mismatched_spawner = Bindings {
        tasks: harness.tasks.clone(),
        tools: harness.tools.clone(),
        scope: harness.scope.clone(),
        concurrency: harness.concurrency,
        state: harness.state.clone(),
        spawner: Some(Arc::new(SplitState {
            task_id,
            record: changed_request,
        })),
        ..Bindings::local()
    }
    .build(None)?;
    assert!(matches!(
        mismatched_spawner.reconcile_admission(operation_id).await,
        Err(Error::Conflict(_))
    ));
    let mut changed_schema = record;
    changed_schema.input_schema =
        serde_json::json!({"type": "object", "additionalProperties": true});
    assert!(matches!(
        harness
            .validate_observed_admission(&changed_schema, None)
            .await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn durable_reattachment_requires_the_exact_admitted_definition() -> Result<()> {
    let mut tasks = TaskRegistry::default();
    tasks.register(resumable_fixture("test.first", [1; 32])?)?;
    tasks.register(resumable_fixture("test.second", [2; 32])?)?;
    let first = tasks.get::<Value, u32>("test.first")?;
    let operation_id = OperationId::from_bytes([5; 16]);
    let scope = RuntimeScope::default();
    let record = tasks.registered_admission(
        operation_id,
        "test.first",
        "1",
        serde_json::json!({}),
        None,
        None,
        &scope,
        &mut None,
    )?;
    let harness = Bindings {
        tasks,
        scope,
        concurrency: 1,
        durable_host: Some(Arc::new(SplitState {
            task_id: TaskId::from_bytes([5; 16]),
            record,
        })),
        ..Bindings::local()
    }
    .build(None)?;
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
    Ok(())
}

#[test]
fn private_root_directory_grant_reads_descendants_without_write_authority() -> Result<()> {
    let file = file_fixture("notes/nested/message.txt", "text/plain")?;
    let volume = file.volume();
    let grants = Capabilities::new([volume.directory_read_capability("", &mut None)?]);
    assert!(read_granted(&grants, &file)?);
    assert!(!grants.contains(&volume.capability(VolumeOperation::Write, &mut None)?));
    for capability in [
        file.read_capability(&mut None)?,
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.directory_read_capability("notes", &mut None)?,
        volume.directory_read_capability("notes/nested", &mut None)?,
    ] {
        let grants = Capabilities::new([capability]);
        assert!(read_granted_with_work(&grants, &file, &mut None)?);
    }
    let unrelated = Capabilities::new([
        volume.directory_read_capability("notes/other", &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    assert!(!read_granted_with_work(&unrelated, &file, &mut None)?);
    Ok(())
}

#[tokio::test]
async fn pinned_task_versions_coexist_without_implicit_latest_selection() -> Result<()> {
    let mut tasks = TaskRegistry::default();
    for (version, delta) in [("1", 1), ("2", 2)] {
        tasks.register(TaskDefinition::live(
            "versioned",
            version,
            move |_, value: u32| async move { Ok(value + delta) },
        )?)?;
    }
    assert!(matches!(
        tasks.get::<u32, u32>("versioned"),
        Err(Error::Conflict(_))
    ));
    let harness = local_runtime(tasks, &[], 2)?;
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
    let harness = crate::runtime::Bindings {
        scope: root,
        concurrency: 1,
        ..crate::runtime::Bindings::local()
    }
    .build(None)?;
    let view = harness.scoped(Capabilities::new([] as [String; 0]), narrowed)?;
    assert!(harness.scope.grants().contains("tool:call:test.echo"));
    assert!(!view.scope.grants().contains("tool:call:test.echo"));
    assert!(
        view.scoped(Capabilities::new(["tool:call:test.echo"]), narrowed)
            .is_err()
    );
    Ok(())
}
