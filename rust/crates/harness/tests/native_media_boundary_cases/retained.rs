use super::*;

#[allow(
    clippy::too_many_arguments,
    clippy::cognitive_complexity,
    reason = "one retained original-admission scenario includes reopen and mutation controls"
)]
pub(super) async fn exercise_retained_native_boundary<A, O>(
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    signed: Scope,
    scope: RuntimeScope,
    reader: Arc<ReaderSpy>,
    selected: Vec<(FileRef, NativeMediaPolicy)>,
    writer: &FilesystemContentPublisher<A, O>,
) -> Result<()>
where
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let machine: Arc<dyn ResumableMachine> = Arc::new(ReplayMachine {
        identity: MachineIdentity {
            name: "fixture.native_replay".into(),
            version: "1".into(),
            digest: [114; 32],
        },
        schema: json!({"type":"integer"}),
    });
    let definition = TaskDefinition::<u64, u64>::resumable(
        machine.clone(),
        json!({"type":"integer"}),
        json!({"type":"integer"}),
    )?;
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let definition = tasks.get_version::<u64, u64>(
        "fixture.native_replay",
        "1",
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        None,
    )?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let stream = Arc::new(MemoryStream::default());
    let admission = OperationId::from_bytes([115; 16]);
    let task = TaskId::from_bytes(admission.into_bytes());
    let issuer = AuthorityIssuer::new(
        "native-replay-task",
        [23; 32],
        Authority {
            kind: AggregateKind::Task,
            id: task.to_string(),
        },
    );
    let signed = issuer
        .issue(
            "native-replay-task-owner",
            signed.capabilities(),
            Some(
                signed
                    .agent()
                    .ok_or_else(|| Error::Unauthorized("original Agent identity absent".into()))?,
            ),
            None,
            &mut None,
        )
        .expect("scope proof");
    let turn = OperationId::from_bytes([116; 16]);
    let mut model = MockAdapter::new(reader);
    model.receipts_enabled = true;
    model.interrupt_after_capture.store(true, Ordering::SeqCst);
    let adapter = Arc::new(model);
    let content = ModelContent::Parts(
        selected
            .iter()
            .map(|(file, policy)| ModelContentPart::File {
                file: file.clone(),
                policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
            })
            .collect(),
    );
    let conversion = explicit_conversion::conversion_tool()?;
    let mut conversion_tools = ToolRegistry::default();
    conversion_tools.register((conversion.clone(), None), &mut None, None)?;
    let conversion_journal = Arc::new(FilesystemExecutionJournal::new(
        stream.clone(),
        host.clone(),
        &acyclic_harness::core::SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None).expect("verifier copy"),
            scope: &signed,
            maximum_bytes: 65_536,
            io: None,
        },
    )?);
    let converter = DurableToolRunner::new(&conversion_tools, conversion_journal, None)?;
    let conversion_operation = OperationId::from_bytes([117; 16]);
    let source = selected
        .first()
        .ok_or_else(|| Error::NotFound("conversion source".into()))?
        .0
        .clone();
    let conversion_input = explicit_conversion::ConvertImage {
        source: source.clone(),
        destination: "derived/image.img1".into(),
        mask: 255,
    };
    let mut retained_conversion = None;
    let mut fence = None;
    let mut first_capture = None;
    let mut completed = None;
    for pass in 0..3 {
        let runtime = FilesystemTaskRuntime::open(
            stream.clone(),
            host.clone(),
            &volume,
            Some(&volume),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            &scope,
            &tasks,
            &machines,
            &ToolRegistry::default(),
            SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 1,
                // Replay, compaction summary, parent response and child response.
                model_steps: 4,
            },
            1,
            65_536,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            None,
            &[],
            &acyclic_harness::durable_tool::ResumableToolRegistry::default(),
            None,
        )
        .await?;
        if pass == 0 {
            assert!(matches!(
                runtime
                    .harness()
                    .admit(admission, &definition, 7, None)
                    .await?,
                Admission::Accepted(_)
            ));
            let worker = Worker {
                id: "native-replay".into(),
                available: ResourceRequest::default(),
                labels: BTreeMap::new(),
            };
            let lease = match runtime
                .task_host()
                .pull_work(&worker, None, &mut None)
                .await?
            {
                WorkPull::Claimed(lease) => lease,
                WorkPull::Idle => return Err(Error::NotFound("original task lease".into())),
                WorkPull::Unresolved { error, .. } => return Err(error),
            };
            runtime.task_host().start_task(&lease, &mut None).await?;
            fence = Some(LeaseFence::from(&lease.reservation));
        }
        let original = runtime.task_host().observe_admission(task).await?;
        assert_eq!(original.extensions.as_ref(), scope.extensions());
        let context = runtime
            .harness()
            .durable_context(task, admission, None)
            .await?;
        assert_eq!(context.scope().extensions(), scope.extensions());
        let writable = acyclic_harness::runtime::Bindings {
            tasks: tasks.clone(),
            tools: conversion_tools.clone(),
            scope: scope.clone(),
            concurrency: 1,
            durable_host: Some(runtime.task_host().clone()),
            interactions: None,
            content: Some(ContentBindings {
                reader: adapter.reader.clone(),
                writer: Some(FilesystemContentPublisher::new(
                    host.clone(),
                    &volume,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    &signed,
                    65_536,
                    None,
                )?),
            }),
            artifacts: None,
            policy: None,
            ..acyclic_harness::runtime::Bindings::local()
        }
        .build(None)?
        .durable_context(task, admission, None)
        .await?;
        let arguments = serde_json::to_value(&conversion_input)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let outcome = converter
            .run(
                task,
                &conversion.definition,
                &acyclic_harness::tool::ToolInvocation {
                    operation_id: conversion_operation,
                    call_id: conversion_operation.to_string(),
                    name: conversion.definition.name.clone(),
                    arguments: arguments,
                },
                &ToolContext::new(
                    writable.clone(),
                    conversion_operation,
                    &(conversion_operation.to_string()),
                    None,
                )?,
                None,
                false,
            )?
            .await?
            .0;
        let Outcome::Succeeded(value) = outcome else {
            return Err(Error::Invalid(format!(
                "explicit conversion did not succeed: {outcome:?}"
            )));
        };
        let converted: explicit_conversion::ConvertedImage =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(converted.source, source);
        assert_eq!(converted.mask, 255);
        let original_bytes = writable.read_file(&source, None).await?;
        let derived_bytes = writable.read_file(&converted.derived, None).await?;
        let mut expected_bytes = original_bytes.as_ref().to_vec();
        for pixel in expected_bytes.iter_mut().skip(8) {
            *pixel ^= 255;
        }
        assert_eq!(derived_bytes.as_ref(), expected_bytes.as_slice());
        if let Some(retained) = &retained_conversion {
            assert_eq!(&converted, retained);
        }
        retained_conversion = Some(converted.clone());
        let mut changed = conversion_input.clone();
        changed.mask = 1;
        assert!(
            converter
                .run(
                    task,
                    &conversion.definition,
                    &acyclic_harness::tool::ToolInvocation {
                        operation_id: conversion_operation,
                        call_id: conversion_operation.to_string(),
                        name: conversion.definition.name.clone(),
                        arguments: serde_json::to_value(changed)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    },
                    &ToolContext::new(
                        writable.clone(),
                        conversion_operation,
                        &(conversion_operation.to_string()),
                        None
                    )?,
                    None,
                    false,
                )?
                .await
                .map(|result| result.0)
                .is_err()
        );
        let invocation = ToolInvocation {
            operation_id: conversion_operation,
            call_id: conversion_operation.to_string(),
            name: conversion.definition.name.clone(),
            arguments: serde_json::to_value(&conversion_input)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let projection: ToolResultContent = serde_json::from_value(
            conversion
                .projection
                .project(
                    &invocation,
                    &ToolResult {
                        value: serde_json::to_value(&converted)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    },
                    None,
                )?
                .0,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let ToolResultContent::Parts { parts } = &projection else {
            return Err(Error::Invalid("derived parts".into()));
        };
        let Some(ModelDataPart::File {
            file,
            policy: FileProjectionPolicy::Native(policy),
        }) = parts.first()
        else {
            return Err(Error::Invalid("derived native file".into()));
        };
        let mut expected = selected.clone();
        expected.push((file.clone(), policy.as_ref().clone()));
        let mut converted_content = content.parts().to_vec();
        converted_content.push(ModelContentPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::Native(policy.clone()),
        });
        let execution = runtime
            .stock_execution(
                task,
                fence
                    .clone()
                    .ok_or_else(|| Error::NotFound("original lease".into()))?,
                turn,
                Model::new(
                    "fixture-native".into(),
                    "all".into(),
                    "mock-native-1".into(),
                    Value::Null,
                )?,
                adapter.clone(),
                ContextPipeline::default(),
                ToolRegistry::default(),
                None,
            )
            .await?
            .with_max_output_tokens(16)?;
        let input = TurnInput {
            operation_id: execution.operation_id(),
            input: ModelContent::Parts(converted_content),
            selected_context: None,
            max_steps: 1,
        };
        let result = execution.execute(input).await;
        if pass == 0 {
            assert!(matches!(result, Err(Error::Storage(_))), "{result:?}");
            assert_eq!(adapter.generates.load(Ordering::SeqCst), 1);
            assert_eq!(adapter.reconciles.load(Ordering::SeqCst), 0);
            let capture = adapter
                .captures
                .lock()
                .map_err(|_| Error::Storage("capture lock".into()))?
                .first()
                .cloned()
                .ok_or_else(|| Error::NotFound("native dispatch capture".into()))?;
            let media = capture
                .parts
                .iter()
                .filter_map(|part| {
                    if let CapturedPart::Media(media) = part {
                        Some(media)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(media.len(), expected.len());
            assert_eq!(
                media
                    .last()
                    .ok_or_else(|| Error::NotFound("derived capture".into()))?
                    .bytes
                    .as_ref(),
                derived_bytes.as_ref()
            );
            for (captured, (file, policy)) in media.iter().zip(&expected) {
                assert_eq!(&captured.file, file);
                assert_eq!(&captured.policy, policy);
                assert_eq!(
                    captured.options,
                    policy
                        .configuration
                        .as_ref()
                        .map(|_| json!({"mode":"strict","scale":2}))
                );
            }
            first_capture = Some(capture);
        } else {
            let output = result?;
            if let Some(previous) = &completed {
                assert_eq!(&output, previous);
            }
            completed = Some(output);
            assert_eq!(adapter.generates.load(Ordering::SeqCst), 1);
            assert_eq!(adapter.reconciles.load(Ordering::SeqCst), 1);
            let captures = adapter
                .captures
                .lock()
                .map_err(|_| Error::Storage("capture lock".into()))?;
            assert_eq!(captures.len(), 1);
            assert_eq!(captures.first(), first_capture.as_ref());
        }
        drop(execution);
        if pass == 2 {
            exercise_media_compaction_fork_restart(
                host.clone(),
                volume.clone(),
                adapter.reader.clone(),
                &expected,
                (invocation, projection),
                &context,
                (
                    runtime.task_host(),
                    &issuer,
                    &signed,
                    fence
                        .as_ref()
                        .ok_or_else(|| Error::NotFound("original lease".into()))?,
                ),
            )
            .await?;
        }
        drop(runtime);
        if pass == 0 {
            writer
                .stage(
                    OperationId::new(),
                    "media/0.bin",
                    b"replaced mutable media",
                    "application/octet-stream",
                    "replacement",
                    acyclic_harness::conversation::ContentAccess::default(),
                )?
                .await?
                .0;
            writer
                .stage(
                    OperationId::new(),
                    "options.json",
                    br#"{"mode":"strict","scale":4}"#,
                    "application/json",
                    "replacement options",
                    acyclic_harness::conversation::ContentAccess::default(),
                )?
                .await?
                .0;
        }
    }
    Ok(())
}
