use super::*;
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
        fixture::<Vec<String>>("grants", json!({}))?
            .into_iter()
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
    let mut config: Config = fixture(
        "config",
        json!({
            "filesystem_root": root.join("filesystem"), "stream_root": root.join("stream"),
            "filesystem_provider": filesystem_provider, "stream_provider": stream_provider,
            "private_volume": private, "owner": owner,
            "generation": format!("qualification-{}", uuid::Uuid::new_v4()),
            "model_invocation_ledger": root.join("model-invocations.jsonl"),
            "session_limits": fixture::<SessionLimits>("session_limits", json!({"total_tasks": total_tasks}))?,
            "tools": crate::native_tools::NativeToolsConfig::default(),
        "context": crate::native::WorkflowContext::default()
        }),
    )?;
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
    let imported_generation = imported
        .head(acyclic_fs::WorkBudget::UNBOUNDED, cancellation)
        .await
        .map_err(|failure| storage(failure.error))?
        .value;
    imported_generation
        .pin(
            "graphcoder-native-original-baseline",
            acyclic_fs::WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|failure| storage(failure.error))?;
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
        let input: ModelTaskCommand = fixture(
            "model_input",
            json!({
                "input": format!("real isolated {label} qualification task")
            }),
        )?;
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
    let aggregate = aggregate(&stream, files.clone(), &owner, &config).await?;
    let mut seed = Caller {
        task: seed_task,
        binding: owner,
        project,
        private,
        result: None,
        aggregate,
        lease: seed_lease,
    };
    seed.apply(Action::BindConversation { agent: seed_agent })
        .await?;
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
    let mut callers = vec![(seed, "bootstrap_seed")];
    for (label, parent, messages) in [
        ("root", 0, 17),
        ("left", 1, 2),
        ("right", 1, 1),
        ("grandchild", 2, 1),
    ] {
        let mut child = producer
            .fork(&mut callers[parent].0, label, None)
            .await?
            .ok_or_else(|| Error::Conflict(format!("{label} fork was not published")))?;
        producer.seed(&mut child, label, messages).await?;
        callers.push((child, label));
    }
    let source = callers[1].0.task;
    producer
        .fork(&mut callers[2].0, "cross_source", Some(source))
        .await?;
    let mut identities = BTreeMap::new();
    let mut caller_records = BTreeMap::new();
    for (caller, label) in &callers {
        let label = *label;
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
    let seed = &callers[0].0;
    let logical_root = &callers[1].0;
    let right = &callers[3].0;
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
        .pin(
            "graphcoder-native-exact-approved-target",
            acyclic_fs::WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|failure| storage(failure.error))?;
    let publication_operation = OperationId::new();
    let source_root_identity = acyclic_fs::capture_root_identity(&native_root)
        .map_err(|error| {
            Error::Unsupported(format!(
                "actual imported root identity is unavailable: {error}"
            ))
        })?
        .to_bytes();
    let native_binding: NativePublicationConfig = fixture(
        "publication",
        json!({
            "operation_id": publication_operation, "caller": logical_root.task,
            "caller_lease": logical_root.lease, "owner": logical_root.binding,
            "private_volume": logical_root.private, "root": native_root,
            "operation_directory": native_operation_directory, "state_root": root.join("native-publication-state"),
            "source_root_identity": source_root_identity, "from_project": seed.project, "to_project": logical_root.project,
            "from": imported_generation.id(), "to": target_generation.id()
        }),
    )?;
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
