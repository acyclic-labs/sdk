use super::*;

pub(super) async fn exercise_live_native_boundary() -> Result<()> {
    let provider = ProviderRef::new(
        "native-boundary".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let agent_id = AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "native-boundary".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent_id),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: "native-boundary".into(),
    };
    let issuer = AuthorityIssuer::new("native-boundary", [21; 32], authority.clone());
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let write = volume.capability(VolumeOperation::Write, &mut None)?;
    let owner = issuer
        .issue(
            "owner",
            &Capabilities::new([
                read.clone(),
                write,
                "extension:configure".into(),
                "extension:activate".into(),
                "operation:declare".into(),
                "operation:observe".into(),
                "operation:cancel".into(),
                "model:generate".into(),
                "task:spawn:fixture.native_replay@1".into(),
                "tool:call:example.convert_image".into(),
            ]),
            Some(agent_id),
            None,
            &mut None,
        )
        .expect("scope proof");
    let writer = FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &owner,
        65_536,
        None,
    )?;
    let options_bytes = br#"{"mode":"strict","scale":2}"#;
    let options_file = writer
        .stage(
            OperationId::new(),
            "options.json",
            options_bytes,
            "application/json",
            "options.json",
            None,
        )?
        .await?
        .0;
    let extension = ExtensionDependency {
        name: "fixture.media".into(),
        version: 1,
    };
    let mut schemas = SchemaRegistry::new();
    schemas.register(extension.name.clone(),1,json!({"type":"object"}),[31;32],ExtensionForkPolicy::Inherit,[],Some(json!({"type":"object","required":["mode","scale"],"properties":{"mode":{"const":"strict"},"scale":{"type":"integer","minimum":1,"maximum":4}},"additionalProperties":false})))?;
    let mut agent = Reducer::new(
        authority,
        issuer.verifier(&mut None).expect("verifier copy"),
        schemas.clone(),
    );
    agent
        .schemas()
        .validate_configuration_bytes(&extension, &options_file, options_bytes, None)?;
    for (revision, action) in [
        (
            0,
            Action::ConfigureExtension {
                extension: extension.clone(),
                content: (options_file).into(),
            },
        ),
        (
            1,
            Action::SelectExtensions {
                roots: vec![extension.clone()],
            },
        ),
    ] {
        agent.apply(&Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new(format!("native:{revision}"))?,
            expected_revision: revision,
            scope: owner.clone(),
            causal_parent: None,
            action,
        })?;
    }
    let admission = agent
        .extension_admission()?
        .ok_or_else(|| Error::NotFound("native selection".into()))?;
    let binding = NativeConfigurationBinding {
        source: admission.source().clone(),
        configuration: admission
            .configurations()
            .first()
            .ok_or_else(|| Error::NotFound("native configuration".into()))?
            .clone(),
        implementation_digest: [31; 32],
    };
    let extensions = ExtensionRegistry::default();
    extensions.install(Arc::new(FixtureExtension))?;
    let linked = ExtensionRuntime::from_reducer(extensions, &agent)?;
    let bodies = [
        [b"IMG1".as_slice(), &4u32.to_le_bytes(), &[0, 127, 255, 1]].concat(),
        [
            b"AUD1".as_slice(),
            &8u32.to_le_bytes(),
            &80u32.to_le_bytes(),
            &[0; 16],
        ]
        .concat(),
        [
            b"VID1".as_slice(),
            &2u32.to_le_bytes(),
            &10u32.to_le_bytes(),
            &2u32.to_le_bytes(),
            &[1, 2, 3, 4],
        ]
        .concat(),
        [
            b"DOC1".as_slice(),
            &2u32.to_le_bytes(),
            &3u32.to_le_bytes(),
            b"one",
            &3u32.to_le_bytes(),
            b"two",
        ]
        .concat(),
    ];
    let intents = [
        NativeMediaIntent::Image {
            detail: ImageDetail::High,
        },
        NativeMediaIntent::Audio {
            maximum_duration_ms: 1_000,
        },
        NativeMediaIntent::Video {
            maximum_duration_ms: 1_000,
            maximum_frames: 16,
        },
        NativeMediaIntent::Document { maximum_pages: 16 },
    ];
    let mut selected = Vec::new();
    for (index, body) in bodies.iter().enumerate() {
        selected.push((
            writer
                .stage(
                    OperationId::new(),
                    &format!("media/{index}.bin"),
                    body,
                    "application/x-harness-native-fixture",
                    "native.bin",
                    None,
                )?
                .await?
                .0,
            policy(
                intents
                    .get(index)
                    .ok_or_else(|| Error::Invalid("fixture intent".into()))?
                    .clone(),
                &binding,
            ),
        ));
    }
    let reader = Arc::new(ReaderSpy {
        inner: FilesystemContentVerifier::new(
            host.clone(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &owner,
            65_536,
            None,
        )?,
        reads: AtomicUsize::new(0),
        corrupt: AtomicBool::new(false),
        missing: AtomicBool::new(false),
    });
    let adapter = Arc::new(MockAdapter::new(reader.clone()));
    let projector = Arc::new(NativeProjector(selected.iter().skip(2).cloned().collect()));
    let definition = ToolDefinition {
        name: "fixture.media_tool".into(),
        revision: "native-1".into(),
        description: "Explicit native fixture projection".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"object","required":["files"]}),
        projection_schema: json!({"type":"object","required":["kind","parts"],"properties":{"kind":{"const":"parts"},"parts":{"type":"array"}},"additionalProperties":false}),
    };
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            Tool {
                hook_names: &[],
                definition: definition.clone(),
                executor: projector.clone(),
                projection: projector.clone(),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "native-tool".into(),
        name: definition.name.clone(),
        arguments: json!({}),
    };
    let canonical = ToolResult {
        value: json!({"files":selected.iter().skip(2).map(|(file,_)|file.clone()).collect::<Vec<_>>()}),
    };
    let tool_content: ToolResultContent =
        serde_json::from_value(projector.project(&invocation, &canonical, None)?.0)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut messages = vec![ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Parts(vec![
            ModelContentPart::Text {
                text: "before".into(),
            },
            ModelContentPart::File {
                file: selected
                    .first()
                    .ok_or_else(|| Error::NotFound("image".into()))?
                    .0
                    .clone(),
                policy: FileProjectionPolicy::Native(Box::new(
                    selected
                        .first()
                        .ok_or_else(|| Error::NotFound("image policy".into()))?
                        .1
                        .clone(),
                )),
            },
            ModelContentPart::Text {
                text: "between".into(),
            },
            ModelContentPart::File {
                file: selected
                    .get(1)
                    .ok_or_else(|| Error::NotFound("audio".into()))?
                    .0
                    .clone(),
                policy: FileProjectionPolicy::Native(Box::new(
                    selected
                        .get(1)
                        .ok_or_else(|| Error::NotFound("audio policy".into()))?
                        .1
                        .clone(),
                )),
            },
        ]),
    }];
    messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(
            acyclic_harness::model::ToolCall::new(
                invocation.call_id.clone(),
                invocation.name.clone(),
                invocation.arguments.clone(),
            ),
        )),
    });
    messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: invocation.call_id,
            name: invocation.name,
            content: tool_content,
        }),
    });
    let mut tasks = TaskRegistry::default();
    let unsupported_file = selected
        .first()
        .ok_or_else(|| Error::NotFound("image for option capability".into()))?
        .0
        .clone();
    let durable_scope = RuntimeScope::new(owner.capabilities().clone(), Limits::default())?
        .with_extensions_from(&agent)?
        .with_extension_runtime(linked.clone())?;
    let test_adapter = adapter.clone();
    let test_reader = reader.clone();
    let retained_host = host.clone();
    let retained_volume = volume.clone();
    let retained_owner = owner.clone();
    let retained_writer = writer.clone();
    tasks.register(TaskDefinition::live(
        "fixture.native_capture",
        "1",
        move |context, (): ()| {
            let adapter = test_adapter.clone();
            let reader = test_reader.clone();
            let selected = selected.clone();
            let messages = messages.clone();
            let bodies = bodies.clone();
            let host = retained_host.clone();
            let volume = retained_volume.clone();
            let owner = retained_owner.clone();
            let writer = retained_writer.clone();
            let scope = durable_scope.clone();
            async move {
                context.model_events(messages.clone(), Some(16)).await?;
                assert_eq!(adapter.counts.load(Ordering::SeqCst), 1);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), 1);
                let capture = adapter
                    .captures
                    .lock()
                    .map_err(|_| Error::Storage("capture lock".into()))?
                    .first()
                    .ok_or_else(|| Error::NotFound("adapter capture".into()))?
                    .clone();
                assert_eq!(capture.adapter_revision, "mock-native-1");
                assert_eq!(
                    capture.request_digest,
                    *blake3::hash(&capture.request_bytes).as_bytes()
                );
                let decoded: ModelRequest = serde_json::from_slice(&capture.request_bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert_eq!(decoded.messages, messages);
                assert_eq!(decoded.max_output_tokens, Some(16));
                assert_eq!(capture.parts.iter().map(|part| match part {
                    CapturedPart::Text(_) => "text", CapturedPart::Call(..) => "call",
                    CapturedPart::Result(..) => "result", CapturedPart::Json(_) => "json",
                    CapturedPart::Media(..) => "media",
                    CapturedPart::Reasoning(..) => "reasoning",
                }).collect::<Vec<_>>(), vec!["text", "media", "text", "media", "call", "result", "text", "media", "media"]);
                assert_eq!(
                    capture
                        .parts
                        .iter()
                        .filter_map(|part| if let CapturedPart::Text(text) = part {
                            Some(text.as_str())
                        } else {
                            None
                        })
                        .collect::<Vec<_>>(),
                    vec!["before", "between", "tool-native"]
                );
                let media = capture
                    .parts
                    .iter()
                    .filter_map(|part| {
                        if let CapturedPart::Media(media) = part {
                            Some((&media.file, &media.policy, &media.bytes, &media.options_bytes, &media.options, &media.measurement))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(media.len(), 4);
                for (index, (file, policy, bytes, option_bytes, options, measured)) in
                    media.iter().enumerate()
                {
                    let expected = selected
                        .get(index)
                        .ok_or_else(|| Error::NotFound("selected media".into()))?;
                    assert_eq!(*file, &expected.0);
                    assert_eq!(*policy, &expected.1);
                    assert_eq!(
                        bytes.as_ref(),
                        bodies
                            .get(index)
                            .ok_or_else(|| Error::NotFound("media body".into()))?
                            .as_slice()
                    );
                    assert_eq!(option_bytes.as_deref(), Some(options_bytes.as_slice()));
                    assert_eq!(*options, &Some(json!({"mode":"strict","scale":2})));
                    assert_eq!(*measured, &measure(bytes, &policy.intent)?);
                }
                assert!(capture.parts.contains(&CapturedPart::Call(
                    "native-tool".into(),
                    "fixture.media_tool".into()
                )));
                assert!(capture.parts.contains(&CapturedPart::Result(
                    "native-tool".into(),
                    "fixture.media_tool".into()
                )));
                for (file, native) in &selected {
                    context
                        .model_events(only_file(file, native), Some(16))
                        .await?;
                    let captures = adapter.captures.lock().map_err(|_| Error::Storage("capture lock".into()))?;
                    let last = captures.last().ok_or_else(|| Error::NotFound("media-only capture".into()))?;
                    assert_eq!(last.parts.len(), 1);
                    assert!(matches!(last.parts.first(), Some(CapturedPart::Media(captured)) if &captured.file == file));
                }
                for (file, native) in &selected {
                    let mut bounded = native.clone();
                    bounded.maximum_work = 1;
                    assert!(matches!(
                        context
                            .model_events(only_file(file, &bounded), Some(16))
                            .await,
                        Err(Error::Invalid(_))
                    ));
                }
                for (index, intent) in [
                    (
                        1,
                        NativeMediaIntent::Audio {
                            maximum_duration_ms: 50,
                        },
                    ),
                    (
                        2,
                        NativeMediaIntent::Video {
                            maximum_duration_ms: 100,
                            maximum_frames: 16,
                        },
                    ),
                    (
                        2,
                        NativeMediaIntent::Video {
                            maximum_duration_ms: 1_000,
                            maximum_frames: 1,
                        },
                    ),
                    (3, NativeMediaIntent::Document { maximum_pages: 1 }),
                ] {
                    let (file, original) = selected
                        .get(index)
                        .ok_or_else(|| Error::NotFound("measured native input".into()))?;
                    let mut native = original.clone();
                    native.intent = intent;
                    let generated = adapter.generates.load(Ordering::SeqCst);
                    let captured = adapter
                        .captures
                        .lock()
                        .map_err(|_| Error::Storage("capture lock".into()))?
                        .len();
                    assert!(matches!(
                        context
                            .model_events(only_file(file, &native), Some(16))
                            .await,
                        Err(Error::Invalid(_))
                    ));
                    assert_eq!(adapter.generates.load(Ordering::SeqCst), generated + 1);
                    assert_eq!(
                        adapter
                            .captures
                            .lock()
                            .map_err(|_| Error::Storage("capture lock".into()))?
                            .len(),
                        captured
                    );
                }
                let original = selected
                    .first()
                    .ok_or_else(|| Error::NotFound("image".into()))?;
                let mut unsupported = original.1.clone();
                unsupported.intent = NativeMediaIntent::Audio {
                    maximum_duration_ms: 1_000,
                };
                let generated = adapter.generates.load(Ordering::SeqCst);
                let captures = adapter
                    .captures
                    .lock()
                    .map_err(|_| Error::Storage("capture lock".into()))?
                    .len();
                assert!(matches!(
                    context
                        .model_events(only_file(&original.0, &unsupported), Some(16))
                        .await,
                    Err(Error::Unsupported(_))
                ));
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generated + 1);
                assert_eq!(
                    adapter
                        .captures
                        .lock()
                        .map_err(|_| Error::Storage("capture lock".into()))?
                        .len(),
                    captures
                );
                for change in [0, 1, 2, 3] {
                    let mut native = original.1.clone();
                    if change == 0 {
                        native.maximum_bytes = original.0.descriptor().byte_length() - 1;
                    }
                    if let Some(binding) = native.configuration.as_mut() {
                        match change {
                            1 => binding.source.revision += 1,
                            2 => binding.configuration.schema_digest = [77; 32],
                            3 => binding.implementation_digest = [78; 32],
                            _ => {}
                        }
                    }
                    let reads = reader.reads.load(Ordering::SeqCst);
                    let counts = adapter.counts.load(Ordering::SeqCst);
                    let generates = adapter.generates.load(Ordering::SeqCst);
                    assert!(
                        context
                            .model_events(only_file(&original.0, &native), Some(16))
                            .await
                            .is_err()
                    );
                    assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
                    assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                    assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                }
                let narrowed = context.scoped(
                    Capabilities::new(["model:generate", "tool:call:fixture.media_tool"]),
                    context.scope().limits(),
                )?;
                let reads = reader.reads.load(Ordering::SeqCst);
                let counts = adapter.counts.load(Ordering::SeqCst);
                let generates = adapter.generates.load(Ordering::SeqCst);
                assert!(matches!(
                    narrowed.model_events(messages, Some(16)).await,
                    Err(Error::Unauthorized(_))
                ));
                assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                for limits in [
                    Limits {
                        file_bytes: 8,
                        render_bytes: 4,
                        ..context.scope().limits()
                    },
                    Limits {
                        render_bytes: 4,
                        ..context.scope().limits()
                    },
                ] {
                    let bounded = context.scoped(context.scope().grants().clone(), limits)?;
                    let reads = reader.reads.load(Ordering::SeqCst);
                    let counts = adapter.counts.load(Ordering::SeqCst);
                    let generates = adapter.generates.load(Ordering::SeqCst);
                    assert!(matches!(
                        bounded
                            .model_events(only_file(&original.0, &original.1), Some(16))
                            .await,
                        Err(Error::Invalid(_))
                    ));
                    assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
                    assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                    assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                }
                adapter.wrong_count_digest.store(true, Ordering::SeqCst);
                assert!(
                    context
                        .model_events(only_file(&original.0, &original.1), Some(16))
                        .await
                        .is_err()
                );
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                adapter.wrong_count_digest.store(false, Ordering::SeqCst);
                let counts = adapter.counts.load(Ordering::SeqCst);
                let generates = adapter.generates.load(Ordering::SeqCst);
                let reads = reader.reads.load(Ordering::SeqCst);
                let captured = adapter.captures.lock().map_err(|_| Error::Storage("capture lock".into()))?.len();
                reader.missing.store(true, Ordering::SeqCst);
                assert!(matches!(context.model_events(only_file(&original.0, &original.1), Some(16)).await, Err(Error::NotFound(_))));
                reader.missing.store(false, Ordering::SeqCst);
                assert!(reader.reads.load(Ordering::SeqCst) > reads);
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                assert_eq!(adapter.captures.lock().map_err(|_| Error::Storage("capture lock".into()))?.len(), captured);
                let reads = reader.reads.load(Ordering::SeqCst);
                reader.corrupt.store(true, Ordering::SeqCst);
                assert!(context.model_events(only_file(&original.0, &original.1), Some(16)).await.is_err());
                assert!(reader.reads.load(Ordering::SeqCst) > reads);
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates);
                reader.corrupt.store(false, Ordering::SeqCst);
                context.model_events(only_file(&original.0, &original.1), Some(16)).await?;
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts + 1);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generates + 1);
                Box::pin(exercise_retained_native_boundary(
                    host, volume, owner, scope, reader, selected, &writer,
                )).await?;
                Ok(())
            }
        },
    )?)?;
    let journal_authority = Authority {
        kind: AggregateKind::Conversation,
        id: acyclic_harness::ConversationId::new().to_string(),
    };
    let journal_issuer = AuthorityIssuer::new("native-conversation", [22; 32], journal_authority);
    let journal_scope = journal_issuer
        .issue(
            "conversation-owner",
            &Capabilities::new([
                read.clone(),
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent_id),
            None,
            &mut None,
        )
        .expect("scope proof");
    let journal = Arc::new(FilesystemExecutionJournal::new(
        Arc::new(MemoryStream::default()),
        host.clone(),
        &acyclic_harness::core::SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &journal_issuer.verifier(&mut None).expect("verifier copy"),
            scope: &journal_scope,
            maximum_bytes: 65_536,
            io: None,
        },
    )?);
    let bundle = HarnessBuilder::new()
        .journal(journal.clone())
        .model(
            Model::new(
                "fixture-native".into(),
                "all".into(),
                "mock-native-1".into(),
                Value::Null,
            )?,
            adapter.clone(),
        )
        .content(ContentBindings {
            reader: reader.clone(),
            writer: None,
        })
        .tasks(tasks)
        .replace_tools(tools)
        .extensions_from(&agent)?
        .extensions(linked.clone())
        .grant("model:generate")
        .grant("task:spawn:fixture.native_capture@1")
        .grant("tool:call:fixture.media_tool")
        .grant("tool:call:example.convert_image")
        .grant(read.clone())
        .build()?;
    let next_bytes = br#"{"mode":"strict","scale":3}"#;
    let next = writer
        .stage(
            OperationId::new(),
            "options.json",
            next_bytes,
            "application/json",
            "options.json",
            None,
        )?
        .await?
        .0;
    agent
        .schemas()
        .validate_configuration_bytes(&extension, &next, next_bytes, None)?;
    for (revision, action) in [
        (
            2,
            Action::ConfigureExtension {
                extension: extension.clone(),
                content: (next).into(),
            },
        ),
        (
            3,
            Action::SelectExtensions {
                roots: vec![extension.clone()],
            },
        ),
    ] {
        agent.apply(&Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new(format!("native:{revision}"))?,
            expected_revision: revision,
            scope: owner.clone(),
            causal_parent: None,
            action,
        })?;
    }
    let runtime = bundle.runtime();
    let task = runtime.task::<(), ()>("fixture.native_capture")?;
    assert_eq!(
        runtime.spawn(&task, ()).await?.result().await?,
        Outcome::Succeeded(())
    );
    let current = agent
        .extension_admission()?
        .ok_or_else(|| Error::NotFound("new native selection".into()))?;
    let current_binding = NativeConfigurationBinding {
        source: current.source().clone(),
        configuration: current
            .configurations()
            .first()
            .ok_or_else(|| Error::NotFound("new native options".into()))?
            .clone(),
        implementation_digest: [31; 32],
    };
    let unsupported_native = policy(
        NativeMediaIntent::Image {
            detail: ImageDetail::High,
        },
        &current_binding,
    );
    let mut unsupported_tasks = TaskRegistry::default();
    let option_adapter = adapter.clone();
    unsupported_tasks.register(TaskDefinition::live(
        "fixture.unsupported_options",
        "1",
        move |context, (): ()| {
            let adapter = option_adapter.clone();
            let file = unsupported_file.clone();
            let native = unsupported_native.clone();
            async move {
                let generated = adapter.generates.load(Ordering::SeqCst);
                let counts = adapter.counts.load(Ordering::SeqCst);
                let captures = adapter
                    .captures
                    .lock()
                    .map_err(|_| Error::Storage("capture lock".into()))?
                    .len();
                assert!(matches!(
                    context
                        .model_events(only_file(&file, &native), Some(16))
                        .await,
                    Err(Error::Unsupported(_))
                ));
                assert_eq!(adapter.counts.load(Ordering::SeqCst), counts + 1);
                assert_eq!(adapter.generates.load(Ordering::SeqCst), generated + 1);
                assert_eq!(
                    adapter
                        .captures
                        .lock()
                        .map_err(|_| Error::Storage("capture lock".into()))?
                        .len(),
                    captures
                );
                Ok(())
            }
        },
    )?)?;
    let current_bundle = HarnessBuilder::new()
        .journal(journal)
        .model(
            Model::new(
                "fixture-native".into(),
                "all".into(),
                "mock-native-1".into(),
                Value::Null,
            )?,
            adapter.clone(),
        )
        .content(ContentBindings {
            reader,
            writer: None,
        })
        .tasks(unsupported_tasks)
        .extensions_from(&agent)?
        .extensions(linked)
        .grant("model:generate")
        .grant("task:spawn:fixture.unsupported_options@1")
        .grant(read)
        .build()?;
    let current_runtime = current_bundle.runtime();
    let unsupported_task = current_runtime.task::<(), ()>("fixture.unsupported_options")?;
    assert_eq!(
        current_runtime
            .spawn(&unsupported_task, ())
            .await?
            .result()
            .await?,
        Outcome::Succeeded(())
    );
    let malformed_bytes = br#"{"mode":false,"scale":2}"#;
    let malformed = writer
        .stage(
            OperationId::new(),
            "malformed-options.json",
            malformed_bytes,
            "application/json",
            "malformed-options.json",
            None,
        )?
        .await?
        .0;
    let counts = adapter.counts.load(Ordering::SeqCst);
    let generated = adapter.generates.load(Ordering::SeqCst);
    assert!(
        agent
            .schemas()
            .validate_configuration_bytes(&extension, &malformed, malformed_bytes, None)
            .is_err()
    );
    assert_eq!(adapter.counts.load(Ordering::SeqCst), counts);
    assert_eq!(adapter.generates.load(Ordering::SeqCst), generated);
    Ok(())
}
