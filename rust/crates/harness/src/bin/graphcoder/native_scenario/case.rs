use super::*;
use acyclic_harness::runtime::TaskSpawner as _;
use prost::Message as _;
pub(super) async fn prepare_case(
    root: &Path,
    total_tasks: u64,
    negative_control: Option<PathBuf>,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(root).await.map_err(storage)?;
    let root = tokio::fs::canonicalize(root).await.map_err(storage)?;
    // Never mix qualification identities with a previous or caller-owned store.
    match tokio::fs::create_dir(root.join("inputs")).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(Error::Conflict(
                "scenario inputs already exist; use a fresh directory".into(),
            ));
        }
        Err(error) => return Err(storage(error)),
    }
    let deployment = format!("{ISSUER}.{}", uuid::Uuid::new_v4());
    let filesystem_provider = ProviderRef::new(
        deployment.clone(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let stream_provider =
        ProviderRef::new(deployment.clone(), "stream".into(), "2".into(), &mut None)?;
    let seed_agent = AgentId::new();
    let private = VolumeRef::new(
        filesystem_provider.clone(),
        "bootstrap-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(seed_agent),
        &mut None,
    )?;
    let project = VolumeRef::new(
        filesystem_provider.clone(),
        "bootstrap-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("qualification-project".into()),
        &mut None,
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: format!("qualification-bootstrap-{deployment}"),
    };
    let issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, authority.clone());
    let grants = Capabilities::new(
        COMMON_GRANTS
            .iter()
            .map(|grant| (*grant).to_owned())
            .chain([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                private.capability(VolumeOperation::Read, &mut None)?,
                private.capability(VolumeOperation::Write, &mut None)?,
                project.capability(VolumeOperation::Read, &mut None)?,
                project.capability(VolumeOperation::Write, &mut None)?,
                project.directory_read_capability("", &mut None)?,
            ]),
    );
    let scope = issuer
        .issue(
            "explicit-trusted-local-bootstrap",
            &grants,
            Some(seed_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let mut owner = AuthorityBinding {
        authority,
        issuer: ISSUER.into(),
        key: TEST_KEY,
        scope,
    };
    let limits = Limits {
        file_bytes: PAYLOAD_BYTES,
        path_bytes: 1024,
        attachments: 16,
        render_bytes: 16 * 1024,
        model_steps: 1,
        model_events_per_step: 64,
        tool_calls_per_step: 16,
        context_messages: 64,
    };
    let run_limits = TaskRunLimits {
        concurrency: Some(6),
        max_steps: Some(1),
        deadline_epoch_ms: None,
    };
    let mut config = Config {
        filesystem_root: root.join("filesystem"),
        stream_root: root.join("stream"),
        filesystem_provider: filesystem_provider.clone(),
        stream_provider: stream_provider.clone(),
        private_volume: private.clone(),
        owner: owner.clone(),
        observed_authorities: Vec::new(),
        generation: format!("qualification-{}", uuid::Uuid::new_v4()),
        limits,
        run_limits,
        model_invocation_ledger: root.join("model-invocations.jsonl"),
        session_limits: SessionLimits {
            active_tasks: 6,
            total_tasks,
            depth: 3,
            model_steps: 7,
        },
        concurrency: 6,
        deterministic_delay_ms: 2000,
        fork_sources: Vec::new(),
        tools: Default::default(),
        context: Default::default(),
        native_publications: Vec::new(),
        project_source_root: None,
    };
    // Open the actual diagnostic provider sink before any model invocation.
    drop(crate::provider::Deterministic::new(
        config.deterministic_delay_ms,
        &config.model_invocation_ledger,
    )?);
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&config.filesystem_root))
        .await
        .map_err(storage)?;
    let native_root = root.join("native-project");
    tokio::fs::create_dir(&native_root).await.map_err(storage)?;
    tokio::fs::write(
        native_root.join("sentinel.txt"),
        b"host original imported sentinel\n",
    )
    .await
    .map_err(storage)?;
    tokio::fs::write(
        native_root.join("concurrent-user.txt"),
        b"host original concurrent file\n",
    )
    .await
    .map_err(storage)?;
    let imported = filesystem
        .attach_directory(
            project.storage_name(&mut None)?,
            &native_root,
            acyclic_fs::SourceOptions {
                mode: acyclic_fs::SourceMode::Pinned,
                maximum_paths: 64,
                maximum_extent_spans: 16,
                maximum_queued_changes: 64,
                excluded_paths: Vec::new(),
            },
        )
        .await
        .map_err(|error| {
            Error::Unsupported(format!(
                "genuine bounded native source import failed: {error}"
            ))
        })?;
    let imported_generation = imported.head().await.map_err(storage)?;
    imported_generation
        .pin("graphcoder-native-original-baseline")
        .await
        .map_err(storage)?;
    let native_operation_directory = root.join("native-writeback-operation");
    tokio::fs::create_dir(&native_operation_directory)
        .await
        .map_err(storage)?;
    let files = FilesystemHost::new(filesystem.clone(), &filesystem_provider, None)?;
    files.create_volume(&private).await?;
    files.create_volume(&project).await?;
    // Known qualification prompts are real provider-owned command artifacts.
    let project_writer = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &owner.scope,
        &project,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let mut planned_commands = BTreeMap::new();
    let mut artifact_grants = owner
        .scope
        .capabilities()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for label in [
        "bootstrap",
        "root",
        "left",
        "right",
        "grandchild",
        "cross_source",
    ] {
        let input = ModelTaskCommand {
            input: ModelContent::Text(format!("real isolated {label} qualification task")),
            selected_context: None,
            max_steps: 1,
            max_output_tokens: Some(256),
            mailbox_operation: None,
            selected_output_volume: None,
        };
        let bytes = serde_json::to_vec(&input).map_err(invalid)?;
        let file = files
            .put_content(
                &project,
                &project_writer,
                &format!(".qualification-inputs/{label}.json"),
                &bytes,
                "application/json",
                "model-input.json",
                PAYLOAD_BYTES,
                &IdempotencyKey::new(format!("planned-input-{label}"))?,
                None,
                None,
            )
            .await?;
        artifact_grants.push(file.read_capability(&mut None)?);
        let command = WorkflowCommand {
            operation_id: OperationId::new(),
            kind: MODEL_TASK_COMMAND_KIND.into(),
            payload: file,
        };
        planned_commands.insert(label.to_owned(), command);
    }
    owner.scope = issuer
        .issue(
            "explicit-trusted-local-bootstrap",
            &Capabilities::new(artifact_grants),
            Some(seed_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    config.owner = owner.clone();
    let stream = Arc::new(
        LocalStream::open(&config.stream_root, Default::default())
            .await
            .map_err(storage)?,
    );
    let runtime =
        crate::native::runtime(&config, stream.clone(), files.clone(), owner.verifier()?).await?;
    let definition: Arc<TaskDefinition<WorkflowCommand, TurnOutput>> =
        runtime.harness().task("acyclic.stock_turn@1")?;
    let operation = OperationId::new();
    let command = planned_commands["bootstrap"].clone();
    let seed_task = admitted_task(
        runtime
            .harness()
            .admit(operation, &definition, command, None)
            .await?,
    )?;
    config.project_source_root = Some(seed_task);
    let template = runtime.task_host().observe_admission(seed_task).await?;
    let worker = Worker {
        id: format!("qualification-bootstrap-{}", uuid::Uuid::new_v4()),
        available: Default::default(),
        labels: Default::default(),
    };
    let seed_lease = claim(&runtime, seed_task, &worker).await?;
    let seed_lease_path = root
        .join("inputs")
        .join("bootstrap_seed-original-lease.json");
    persist(&seed_lease_path, &seed_lease).await?;
    let mut aggregate = aggregate(&stream, files.clone(), &owner, &config).await?;
    aggregate
        .execute(
            Command {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("bind-bootstrap")?,
                expected_revision: 0,
                scope: owner.scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent: seed_agent },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let mut seed = Caller {
        task: seed_task,
        binding: owner,
        project,
        private,
        result: None,
        aggregate,
        lease: seed_lease,
    };
    let mut producer = Producer {
        root: root.clone(),
        files,
        stream,
        runtime,
        config,
        template,
        worker,
        preparations: Vec::new(),
        references: BTreeMap::new(),
        leases: BTreeMap::from([("bootstrap_seed".to_owned(), seed_lease_path)]),
        planned_commands,
        cancellation: cancellation.clone(),
    };
    let imported_ref = acyclic_harness::resources::GenerationRef::new(
        &producer.config.filesystem_provider,
        &imported_generation.id().digest().into_bytes(),
        None,
        None,
    )?
    .0;
    let imported_bytes = producer
        .files
        .read(
            &workspace_ref(
                producer.config.filesystem_provider.clone(),
                &seed.project.storage_name(&mut None)?,
            )?,
            Some(&imported_ref),
            "/sentinel.txt",
            PAYLOAD_BYTES,
        )
        .await?;
    let imported_file = FileRef::new(
        seed.project.clone(),
        "sentinel.txt".into(),
        (hex::encode(imported_generation.id().digest().into_bytes())).into(),
        acyclic_harness::conversation::FileDescriptor::from_bytes(
            &imported_bytes,
            "text/plain",
            &mut None,
        )?,
        "sentinel.txt".into(),
        &mut None,
    )?;
    producer
        .references
        .insert("native_import_baseline".into(), imported_file);
    producer.seed(&mut seed, "bootstrap", 1).await?;
    let mut logical_root = producer
        .fork(&mut seed, "root", None)
        .await?
        .ok_or_else(|| Error::Conflict("bootstrap root was not published".into()))?;
    producer.seed(&mut logical_root, "root", 17).await?;
    let mut left = producer
        .fork(&mut logical_root, "left", None)
        .await?
        .ok_or_else(|| Error::Conflict("left fork was not published".into()))?;
    producer.seed(&mut left, "left", 2).await?;
    let mut right = producer
        .fork(&mut logical_root, "right", None)
        .await?
        .ok_or_else(|| Error::Conflict("right fork was not published".into()))?;
    producer.seed(&mut right, "right", 1).await?;
    let mut grandchild = producer
        .fork(&mut left, "grandchild", None)
        .await?
        .ok_or_else(|| Error::Conflict("grandchild fork was not published".into()))?;
    producer.seed(&mut grandchild, "grandchild", 1).await?;
    // Authenticated foreign project selection only.
    producer
        .fork(&mut left, "cross_source", Some(logical_root.task))
        .await?;
    let notice_file = write(&producer.files, &left.binding, &left.private,
        "merge/selected-grandchild.txt",
        b"Explicit caller notice: import the selected immutable grandchild project; do not import private scratch.\n",
        "text/plain", "merge-notice.txt", "left-grandchild-merge-notice").await?;
    producer
        .references
        .insert("merge_notice".into(), notice_file.clone());
    let notice_sequence = left
        .aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("merge caller conversation is missing".into()))?
        .logical_revision()
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("merge notice sequence overflow".into()))?;
    producer.config.tools.merges.push(ConfiguredRecursiveMerge {
        caller: left.task,
        source: grandchild.task,
        notice: ConversationMessage {
            id: uuid::Uuid::new_v4(),
            sequence: notice_sequence,
            kind: MessageKind::Merge,
            content: notice_file,
            attachments: Default::default(),
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        },
    });
    let callers = [
        (&seed, "bootstrap_seed"),
        (&logical_root, "root"),
        (&left, "left"),
        (&right, "right"),
        (&grandchild, "grandchild"),
    ];
    let mut identities = BTreeMap::new();
    let mut caller_records = BTreeMap::new();
    for (caller, label) in callers {
        if label != "bootstrap_seed" {
            producer.config.tools.callers.push(NativeToolCaller {
                task: caller.task,
                owner: caller.binding.clone(),
                project: caller.project.clone(),
                private_volume: caller.private.clone(),
                native_snapshot: None,
                result_volume: caller.result.clone(),
            });
        }
        identities.insert(label, caller.task);
        caller_records.insert(label, json!({ "agent": caller.binding.scope.agent(),
            "authority": caller.binding.authority, "project": caller.project, "private_volume": caller.private,
            "admission": producer.runtime.task_host().observe_admission(caller.task).await? }));
    }
    let cursor = acyclic_harness::wire::ReplayCursor {
        authority: Some(acyclic_harness::wire_codec::encode_authority(
            &producer.config.owner.authority,
        )),
        generation: producer.config.generation.clone(),
        revision: 0,
    };
    let cursor_path = root.join("inputs").join("owner-cursor.pb");
    let cursor_frame = acyclic_harness::wire::ClientFrame {
        frame: Some(acyclic_harness::wire::client_frame::Frame::Resume(
            acyclic_harness::wire::ResumeRequest {
                protocol: Some(acyclic_harness::wire_api::current_protocol()),
                cursors: vec![cursor],
            },
        )),
    };
    tokio::fs::write(&cursor_path, cursor_frame.encode_to_vec())
        .await
        .map_err(storage)?;
    let config_path = root.join("native-config.json");
    let target_workspace = filesystem
        .open_workspace(
            logical_root.project.storage_name(&mut None)?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| storage(failure.error))?
        .value;
    let target_generation = target_workspace
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| storage(failure.error))?
        .value;
    target_generation
        .pin("graphcoder-native-exact-approved-target")
        .await
        .map_err(storage)?;
    let publication_operation = OperationId::new();
    let source_root_identity = acyclic_fs::capture_root_identity(&native_root)
        .map_err(|error| {
            Error::Unsupported(format!(
                "actual imported root identity is unavailable: {error}"
            ))
        })?
        .to_bytes();
    let native_binding = NativePublicationConfig {
        operation_id: publication_operation,
        caller: logical_root.task,
        caller_lease: logical_root.lease.clone(),
        owner: logical_root.binding.clone(),
        private_volume: logical_root.private.clone(),
        root: native_root.clone(),
        operation_directory: native_operation_directory.clone(),
        state_root: root.join("native-publication-state"),
        source_root_identity,
        from_project: seed.project.clone(),
        to_project: logical_root.project.clone(),
        from: imported_generation.id(),
        to: target_generation.id(),
        excluded_names: vec![".system".into(), ".qualification-inputs".into()],
        budget: acyclic_fs::WorkBudget {
            authority_records_read: 8192,
            authority_records_appended: 2048,
            authority_bytes_read: 16 * 1024 * 1024,
            authority_bytes_written: 16 * 1024 * 1024,
            object_probes: 8192,
            backend_read_operations: 8192,
            backend_write_operations: 8192,
            durability_operations: 4096,
            page_reads: 8192,
            page_writes: 8192,
            object_bytes_read: 16 * 1024 * 1024,
            object_bytes_written: 16 * 1024 * 1024,
            bytes_hashed: 32 * 1024 * 1024,
            bytes_copied: 32 * 1024 * 1024,
            bytes_encoded: 16 * 1024 * 1024,
            source_bytes_read: 1024 * 1024,
            source_path_components: 4096,
            source_entries_visited: 4096,
            output_bytes: 1024 * 1024,
            items_examined: 16384,
            items_returned: 4096,
            allocation_operations: 65536,
            peak_allocation_bytes: 8 * 1024 * 1024,
            materializations: 64,
        },
        maximum_directory_entries: 64,
        maximum_extent_spans: 16,
        transfer_bytes: PAYLOAD_BYTES,
    };
    native_binding.validate()?;
    producer.config.tools.content_budget = native_binding.budget;
    producer.config.native_publications.push(native_binding);
    let native_command = producer.native_command(&right).await?;
    persist(&config_path, &producer.config).await?;
    let publication_path = root.join("inputs").join("native-publication.json");
    let publication = json!({
        "caller": logical_root.task, "caller_lease": logical_root.lease,
        "root": native_root, "operation_directory": native_operation_directory,
        "state_root": root.join("native-publication-state"),
        "operation_id": publication_operation, "source_root_identity": source_root_identity,
        "from_workspace": seed.project.storage_name(&mut None)?, "from": imported_generation.id(),
        "to_workspace": logical_root.project.storage_name(&mut None)?, "to": target_generation.id(),
        "excluded_names": [".system", ".qualification-inputs"],
        "concurrent_edit_path": native_root.join("concurrent-user.txt"),
        "conflicting_edit_path": native_root.join("sentinel.txt"),
        "target_file": producer.references["root_project"],
        "baseline_file": producer.references["native_import_baseline"]
    });
    persist(
        &publication_path,
        &json!({ "operation_id": publication_operation,
        "caller_lease": logical_root.lease }),
    )
    .await?;
    let manifest = root.join("native-scenario.json");
    persist(&manifest, &json!({
        "format_version": 1,
        "config": config_path, "tasks": identities, "callers": caller_records,
        "model_invocation_ledger": producer.config.model_invocation_ledger,
        "preparations": producer.preparations, "files": producer.references,
        "leases": producer.leases, "cursor_protobuf": cursor_path,
        "native_command": native_command, "native_publication": publication, "native_publication_request": publication_path,
        "session_limits": producer.config.session_limits,
        "negative_control": negative_control,
        "qualification_ready": true,
        "missing_prerequisites": []
    })).await?;
    println!(
        "native-scenario bootstrap=real-provider-precreation manifest={} inputs-ready=true; qualification remains unexecuted",
        manifest.display()
    );
    Ok(manifest)
}
