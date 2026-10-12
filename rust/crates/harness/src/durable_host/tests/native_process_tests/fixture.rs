use super::*;

pub(super) async fn process_fixture<P: StreamProvider + 'static>(
    stream: Arc<P>,
    mode: &str,
    approve: bool,
    work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<ProcessFixture<P>> {
    process_fixture_with_filesystems(
        stream,
        tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?,
        Fs::memory(),
        Fs::memory(),
        mode,
        approve,
        work,
    )
    .await
}

pub(super) fn initialize_issued_view_fixture_services() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
    acyclic_fs::initialize_native_mount_services()
        .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)
        .map_err(Error::from)
}

pub(super) async fn process_fixture_with_filesystems<P, A, O>(
    stream: Arc<P>,
    directory: tempfile::TempDir,
    filesystem: Fs<A, O>,
    auxiliary_filesystem: Fs<A, O>,
    mode: &str,
    approve: bool,
    original_work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<ProcessFixture<P, A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let provider = ProviderRef::new(
        "native-test".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let auxiliary_provider = ProviderRef::new(
        "native-test-aux".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let files = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    let auxiliary_host = FilesystemHost::new(auxiliary_filesystem, &auxiliary_provider, None)?;
    let agent = AgentId::from_bytes([61; 16]);
    let volume = |name: &str, project| {
        VolumeRef::new(
            provider.clone(),
            name.into(),
            if project {
                VolumeClass::Project
            } else {
                VolumeClass::AgentPrivate
            },
            if project {
                VolumeOwner::Project("fixture-project".into())
            } else {
                VolumeOwner::Agent(agent)
            },
            &mut None,
        )
    };
    let source = volume(
        "source",
        matches!(mode, "mixed-issued-view" | "review-test"),
    )?;
    let destination = volume("destination", mode == "review-test")?;
    let second_destination = volume("second-destination", mode == "review-test")?;
    let result_volume = volume("results", false)?;
    let auxiliary = VolumeRef::new(
        auxiliary_provider,
        "auxiliary".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let extra_sources = (0..if mode == "many-bash" { 16 } else { 0 })
        .map(|index| volume(&format!("extra-{index}"), false))
        .collect::<Result<Vec<_>>>()?;
    let command = OperationId::from_bytes([62; 16]);
    let task_id = TaskId::from_bytes([53; 16]);
    let approval = task_interaction_id(task_id, command);
    let issuer = AuthorityIssuer::new(
        "native-test",
        [63; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "native-test-conversation".into(),
        },
    );
    let verifier = issuer.verifier(&mut None)?;
    let mut grants = vec![
        "operation:declare".into(),
        "operation:observe".into(),
        "operation:cancel".into(),
        "task:spawn:test.effect@1".into(),
        "effect:plan".into(),
        "effect:run".into(),
        "effect:provider:test.native".into(),
        "interaction:open".into(),
        "interaction:resolve".into(),
        capability::interaction_respond(approval),
        "interaction:route".into(),
        "conversation:bind".into(),
    ];
    for volume in [
        &source,
        &auxiliary,
        &destination,
        &second_destination,
        &result_volume,
    ]
    .into_iter()
    .chain(&extra_sources)
    {
        grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
    }
    for volume in [&destination, &second_destination, &result_volume] {
        grants.push(volume.capability(VolumeOperation::Write, &mut None)?);
    }
    let grants = Capabilities::new(grants);
    let scope = issuer.issue("owner", &grants, Some(agent), None, &mut None)?;
    let root_births = if mode == "mixed-issued-view" {
        let original = original_work.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "protected mixed Project fixture requires its original native operation",
        })?;
        let config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable);
        let source_operation = acyclic_fs::OperationId::from_bytes([216; 16]);
        let result_operation = acyclic_fs::OperationId::from_bytes([217; 16]);
        let mut initial_generations = [None, None];
        for (index, (volume, operation)) in [
            (&source, source_operation),
            (&result_volume, result_operation),
        ]
        .into_iter()
        .enumerate()
        {
            let name = volume.storage_name_with_native_work(original)?;
            let initial = filesystem
                .create_workspace_with_operation_with_native_work(
                    &name.0,
                    config,
                    operation,
                    original,
                    original.cancellation(),
                )
                .await
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
            initial_generations[index] = Some(initial.id());
            drop(initial);
            drop(name);
        }
        Some((
            config,
            source_operation,
            initial_generations[0].expect("acknowledged Project birth"),
            result_operation,
            initial_generations[1].expect("acknowledged private birth"),
        ))
    } else {
        files.create_volume(&source).await?;
        files.create_volume(&result_volume).await?;
        None
    };
    if mode != "review-test" {
        for volume in [&destination, &second_destination] {
            files.create_volume(volume).await?;
        }
    }
    auxiliary_host.create_volume(&auxiliary).await?;
    for volume in &extra_sources {
        files.create_volume(volume).await?;
    }
    let task = native_task_fixture(
        stream.clone(),
        files.clone(),
        result_volume.clone(),
        &issuer,
        &scope,
        grants,
    )
    .await?;
    let crate::distributed::WorkPull::Claimed(lease) = task
        .task_host()
        .pull_work(&crate::distributed::Worker {
            id: "native-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        })
        .await?
    else {
        return Err(Error::NotFound("native lease".into()));
    };
    task.task_host().start_task(&lease, None).await?;
    let owner_fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let cancellation = acyclic_fs::CancellationToken::new();
    let count = 3 + extra_sources.len() + usize::from(mode == "partial-publication");
    let options = NativeViewOptions {
        root: directory.path().join("view"),
        maximum_volumes: count
            .try_into()
            .map_err(|e: std::num::TryFromIntError| Error::Invalid(e.to_string()))?,
        work_per_volume: acyclic_fs::performance::work_budget(10_000_000, 10_000_000),
    };
    let fallback_work;
    let work = match original_work {
        Some(work) => work,
        None => {
            fallback_work = test_work(options.budget_for_volume_count(count)?, &cancellation)?;
            &fallback_work
        }
    };
    let owner = task
        .task_host()
        .journal_owner_with_native_work(task_id, &owner_fence, work)
        .await?
        .into_native_shared(work)?;
    if let Some((_, _, _, scratch_operation, _)) = root_births {
        use crate::store::history::HistoryIo as _;
        let original = original_work.expect("protected Root births require original work");
        let mut metadata = crate::store::history::native_work_io(original)?;
        metadata.claim(
            std::mem::size_of::<(Authority, AuthorityIssuer, Scope)>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let mut authority = (
            metadata.clone_authority(issuer.audience())?,
            metadata.into_custody(),
        );
        let mut copied_issuer = issuer.clone_with_native_work(original)?;
        authority.1.absorb(&mut copied_issuer.1)?;
        let mut copied_scope = scope.clone_with_native_work(original)?;
        authority.1.absorb(&mut copied_scope.1)?;
        let encoded = crate::filesystem::bind_original_root_conversation_with_native_work(
            &stream,
            (authority.0, copied_issuer.0, copied_scope.0),
            authority.1,
            &owner,
            &result_volume,
            scratch_operation,
            scope.limits(),
            original,
        )
        .await?;
        drop(encoded);
    } else {
        let mut aggregate = crate::store::StreamAggregate::open(
            &owner.stream(),
            issuer
                .verifier(&mut None)
                .expect("verifier copy")
                .audience()
                .clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?;
        aggregate
            .execute(
                crate::core::Command {
                    operation_id: OperationId::from_bytes([64; 16]),
                    idempotency_key: IdempotencyKey::new("native-conversation-bind")?,
                    expected_revision: 0,
                    scope: scope.clone(),
                    causal_parent: None,
                    action: crate::core::Action::BindConversation { agent },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let writes = [&source, &auxiliary]
        .into_iter()
        .chain(&extra_sources)
        .map(|volume| volume.capability(VolumeOperation::Write, &mut None))
        .collect::<Result<Vec<_>>>()?;
    let setup_scope = issuer.issue(
        "setup",
        &Capabilities::new(writes),
        Some(agent),
        None,
        &mut None,
    )?;
    for (host, volume, bytes, key) in [
        (
            &files,
            &source,
            b"pinned input".as_slice(),
            "source-setup".to_owned(),
        ),
        (
            &auxiliary_host,
            &auxiliary,
            b"auxiliary input".as_slice(),
            "auxiliary-setup".to_owned(),
        ),
    ]
    .into_iter()
    .chain(extra_sources.iter().enumerate().map(|(index, volume)| {
        (
            &files,
            volume,
            b"extra input".as_slice(),
            format!("extra-source-{index}"),
        )
    })) {
        let grant = ContentGrant::verify(
            &verifier,
            &setup_scope,
            volume,
            VolumeOperation::Write,
            128,
            &mut None,
        )?;
        host.put_content(
            volume,
            &grant,
            "input.txt",
            bytes,
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new(key)?,
            None,
            None,
        )
        .await?;
    }
    if mode == "review-test" {
        let workspace = workspace_ref(source.provider().clone(), &source.storage_name(&mut None)?)?;
        let generation = files.resolve(&workspace).await?.generation;
        let read = ContentGrant::verify(
            &verifier,
            &scope,
            &source,
            VolumeOperation::Read,
            128,
            &mut None,
        )?;
        for (volume, key) in [
            (&destination, "review-fork"),
            (&second_destination, "caller-fork"),
        ] {
            let write = ContentGrant::verify(
                &verifier,
                &scope,
                volume,
                VolumeOperation::Write,
                128,
                &mut None,
            )?;
            files
                .fork_volume(
                    &source,
                    &read,
                    &generation,
                    volume,
                    &write,
                    None,
                    &IdempotencyKey::new(key)?,
                )
                .await?;
        }
    }
    let mut selections = vec![
        (&files, &source, "src".to_owned(), false),
        (&files, &destination, "dst".to_owned(), true),
        (&auxiliary_host, &auxiliary, "aux".to_owned(), false),
    ];
    if mode == "partial-publication" {
        selections.push((&files, &second_destination, "second-dst".into(), true));
    }
    selections.extend(
        extra_sources
            .iter()
            .enumerate()
            .map(|(index, volume)| (&files, volume, format!("extra-{index}"), false)),
    );
    let mut bindings = Vec::new();
    for (host, volume, path, writable) in selections {
        let workspace = workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
        bindings.push(PreparationSelection {
            host: Arc::clone(host),
            volume: volume.clone(),
            generation: host.resolve(&workspace).await?.generation,
            path,
            writable,
            verifier: verifier.clone(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        });
    }
    let root = directory.path().join("view");
    let operation_directory = directory.path().join("preparation");
    for path in [
        &root,
        &operation_directory,
        &operation_directory.join("target"),
        &operation_directory.join("backup"),
    ] {
        std::fs::create_dir(path).map_err(|e| Error::Storage(e.to_string()))?;
    }
    let borrowed = borrow_preparation_bindings(&bindings);
    let mut lazy_mount = None;
    let mut lazy_work = None;
    let mut project_policy = None;
    let mut project_generation = None;
    let (preparation_root, native_root_custody) =
        NativePreparationRoot::witness(&options.root, &operation_directory, work)?.await?;
    let native_root_custody = Some(native_root_custody);
    let view = {
        let context = task
            .task_context_with_native_work(task_id, &owner_fence, work)
            .await?;
        let guard_bytes = borrowed
            .len()
            .checked_mul(std::mem::size_of::<
                crate::runtime::NativeVolumeCallbackGuard<P>,
            >())
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native source guard slot count overflow",
            })?;
        let custody = work.claim(
            guard_bytes as u64,
            u64::from(guard_bytes != 0),
            Default::default(),
        )?;
        let mut guards = (Vec::with_capacity(borrowed.len()), custody);
        for _ in &borrowed {
            guards.0.push(
                context
                    .volume_callback_guard_with_native_work(&owner, work)
                    .await?,
            );
        }
        let view = if mode == "mixed-issued-view" {
            let volume_work = work.native_volume_work(options.work_per_volume)?;
            let physical = directory.path().join("lazy-source");
            std::fs::create_dir(&physical).map_err(|error| Error::Storage(error.to_string()))?;
            std::fs::write(physical.join("cold.txt"), b"genuine cold project")
                .map_err(|error| Error::Storage(error.to_string()))?;
            let demand = Arc::new(
                acyclic_fs::NativeDemandSource::open(
                    std::borrow::Cow::Owned(std::path::Path::new(&physical).to_path_buf()),
                    acyclic_fs::FilesystemProfile::Portable,
                    acyclic_fs::VolumeLimits::default(),
                    None,
                    None,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(|error| Error::Storage(error.to_string()))?,
            );
            let workspace = files.open_volume_with_native_work(&source, work).await?;
            let lazy = acyclic_fs::LazyWorkspace::open_or_attach(
                workspace,
                demand,
                acyclic_fs::MemoryLazyWorkspaceStore::default(),
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
            let lazy = lazy.into_arc_with_native_work(work)?;
            let callback = guards.0[0]
                .for_volume(&owner, &source, false, &volume_work)?
                .0;
            let (path, _path_custody) =
                volume_work.path(&options.root, Some(std::path::Path::new("src")))?;
            volume_work.create_owned_directory(
                &options.root,
                preparation_root.expected_identity,
                std::path::Path::new("src"),
                true,
            )?;
            let mut allocation = volume_work.claim(
                std::mem::size_of::<
                    acyclic_fs::LazyMount<
                        A,
                        O,
                        acyclic_fs::NativeDemandSource,
                        acyclic_fs::MemoryLazyWorkspaceStore,
                    >,
                >() as u64,
                1,
                Default::default(),
            )?;
            let mut mount = lazy
                .mount(
                    &path,
                    acyclic_fs::MountOptions::read_only()
                        .publication(acyclic_fs::MountPublication::Manual)
                        .native_context(&volume_work, Some(callback), Some(&volume_work)),
                )
                .await?;
            mount.retain_payload_allocation(&mut allocation)?;
            let mount = Arc::new(mount);
            let snapshot = mount
                .snapshot(&volume_work, volume_work.cancellation())
                .await?;
            let native_source =
                crate::filesystem::FsRootNativeSource::from_lazy(Arc::clone(&lazy), work)?;
            let creator = crate::filesystem::FilesystemResourceCreationProvider::new(
                Arc::clone(&files),
                &issuer,
                Some(work),
            )?;
            let (config, operation, initial_generation, _, _) = root_births
                .expect("mixed Project was physically created through its original operation");
            let protected = creator
                .protect_root_bootstrap_with_native_work(
                    &owner,
                    &source,
                    crate::resource_preparation::RootBootstrapContentRole::Project,
                    operation,
                    config,
                    initial_generation,
                    snapshot.authored_generation,
                    Some((&native_source, snapshot)),
                    work,
                )?
                .await?
                .into_native_shared(work)?;
            project_policy = Some(
                crate::filesystem::FsRootProjectSourceProvider::new(
                    Arc::clone(&files),
                    issuer.audience(),
                    task_id,
                    Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
                )?
                .with_bootstrap_source_with_native_work(protected, work)?,
            );
            project_generation = Some(
                crate::resources::GenerationRef::new(
                    borrowed[0].generation.as_resource().provider(),
                    borrowed[0].generation.as_resource().key(),
                    borrowed[0].generation.as_resource().version(),
                    Some(work),
                )
                .map(|(value, custody)| (value, custody.expect("native generation custody")))?,
            );
            let namespace = NativeVolumeNamespace::LazyMount { snapshot };
            let mut mixed_borrowed = borrowed;
            mixed_borrowed[0].namespace = &namespace;
            let held = crate::filesystem::NativeExistingLazyVolume::capture_with_native_work(
                &mount,
                &owner,
                &mixed_borrowed[0],
                &guards.0[0],
                work,
                &volume_work,
            )?
            .await?;
            lazy_mount = Some(Arc::clone(&mount));
            lazy_work = Some(volume_work.clone());
            NativeVolumeView::prepare(
                &owner,
                &guards.0,
                &preparation_root,
                &options,
                &mixed_borrowed,
                &[Some(&held), None, None],
                work,
            )?
            .await?
        } else {
            NativeVolumeView::prepare(
                &owner,
                &guards.0,
                &preparation_root,
                &options,
                &borrowed,
                &vec![None; borrowed.len()],
                work,
            )?
            .await?
        };
        view
    };
    let mut request = NativeProcessRequest {
        executable: std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?,
        argv: vec![
            "--exact".into(),
            "durable_host::tests::native_process_tests::native_process_child".into(),
            "--nocapture".into(),
        ],
        cwd: view.manifest().options.root.clone(),
        environment: BTreeMap::from([
            ("NATIVE_TEST_MODE".into(), mode.into()),
            ("NATIVE_TEST_TOKEN".into(), "approved-env".into()),
        ]),
        timeout_ms: if mode == "timeout" { 300 } else { 10_000 },
        control_timeout_ms: if mode == "timeout" { 50 } else { 250 },
        cancellation_poll_ms: 10,
        maximum_output_bytes: 8192,
        maximum_result_bytes: 65_536,
        mcp_stdio: None,
        standard_input: None,
        view: view.manifest().clone(),
    };
    if mode == "mcp-approval" || mode.starts_with("mcp-stdin-") {
        request.mcp_stdio = Some(crate::mcp::stdio::McpStdioRequest {
            initialization: OperationId::from_bytes([71; 16]),
            operation: OperationId::from_bytes([72; 16]),
            method: crate::mcp::stdio::McpStdioMethod::CallTool,
            params: serde_json::json!({"name":"echo","arguments":{}}),
            maximum_bytes: 4096,
        });
    }
    if mode.starts_with("stdin-") || mode.starts_with("mcp-stdin-") {
        request.standard_input = Some(if mode.ends_with("-empty") {
            Vec::new()
        } else {
            vec![0, 255, b'\n', b'x']
        });
    }
    if mode == "bash" || mode == "mutate-source-bash" || mode == "many-bash" {
        request.executable = if cfg!(windows) {
            "C:/Program Files/Git/bin/bash.exe"
        } else {
            "/bin/bash"
        }
        .into();
        request.argv = vec!["--noprofile".into(), "--norc".into(), "-c".into(),
            "IFS= read -r input < src/input.txt; printf '%s' \"$input\" > dst/output.txt; IFS= read -r aux < aux/input.txt; printf '%s' \"$aux\" > dst/auxiliary.txt; printf '%s' \"$0|$1|$2\"; printf '%s' \"$NATIVE_TEST_TOKEN\" >&2".into(),
            "approved-bash".into(), "first arg".into(), "second arg".into()];
        if mode == "mutate-source-bash" {
            request.argv[3].push_str("; printf 'unauthorized source edit' > src/input.txt");
        }
        if mode == "many-bash" {
            request.argv[3].push_str("; for index in {0..15}; do IFS= read -r value < extra-$index/input.txt; printf '%s|' \"$value\"; done > dst/many.txt");
        }
    }
    let content =
        FilesystemContentVerifier::new(files.clone(), &verifier, &scope, 1_000_000, None)?;
    let results = FilesystemContentPublisher::new(
        files.clone(),
        &result_volume,
        &verifier,
        &scope,
        1_000_000,
        None,
    )?;
    let request_ref = results
        .stage(
            command,
            "process/request.json",
            &crate::contract::canonical_json_bytes(&request)?,
            "application/json",
            "request.json",
        )
        .await?;
    let schema = results
        .stage(
            command,
            "process/schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
        )
        .await?;
    let plan = TaskEffectPlan {
        provider: "test.native".into(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
        request: request_ref,
        result_schema: schema,
    };
    let approvals = Arc::new(FilesystemExecutionJournal::new(
        owner.stream(),
        files.clone(),
        &result_volume,
        &verifier,
        &SchemaRegistry::new(),
        &scope,
        1_000_000,
        None,
    )?);
    approvals
        .open_interaction(
            approval,
            Interaction::approval(
                "Run the exact admitted process".into(),
                command,
                request.approval_digest(owner.task_binding().0, command, None)?,
            )?,
        )
        .await?;
    approvals
        .resolve_interaction(
            approval,
            InteractionResponse::Approval {
                approved: approve,
                reason: None,
            },
            &scope,
        )
        .await?;
    Ok(ProcessFixture {
        directory,
        task,
        lease,
        owner,
        files,
        issuer,
        scope,
        source,
        destination,
        second_destination,
        content,
        results,
        approvals,
        view,
        preparation_root,
        command,
        plan,
        lazy_mount,
        lazy_work,
        project_policy,
        project_generation,
        _native_root_custody: native_root_custody,
    })
}

pub(super) async fn native_task_fixture<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
    grants: Capabilities,
) -> Result<NativeRuntime<P, A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let implementation = Arc::new(NativeEffectMachine {
        identity: MachineIdentity {
            name: "test.effect".into(),
            version: "1".into(),
            digest: [52; 32],
        },
        schema: serde_json::json!({"type":"integer"}),
    });
    let (identity, machine, tasks, machines) = test_registries::<Value>(
        implementation,
        serde_json::json!({"type":"integer"}),
        serde_json::json!({"type":"object"}),
    )?;
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
    let runtime = FilesystemTaskRuntime::open(
        stream,
        files,
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        scope,
        &runtime_scope,
        &tasks,
        &machines,
        &crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        },
        1,
        1_000_000,
        Arc::new(SystemUnixMillisClock),
        None,
        &[],
        &crate::durable_tool::ResumableToolRegistry::default(),
        None,
    )
    .await?;
    runtime
        .task_host()
        .admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: OperationId::from_bytes([53; 16]),
            task: identity,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"object"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
    Ok(runtime)
}

pub(super) fn borrow_preparation_bindings<A, O>(
    bindings: &[PreparationSelection<A, O>],
) -> Vec<BorrowedNativeVolumeBinding<'_, A, O>> {
    bindings
        .iter()
        .map(|binding| BorrowedNativeVolumeBinding {
            host: &binding.host,
            volume: &binding.volume,
            generation: &binding.generation,
            namespace: &NativeVolumeNamespace::MaterializedDirectory,
            path: &binding.path,
            writable: binding.writable,
            verifier: &binding.verifier,
            scope: &binding.scope,
            publication: binding.publication,
        })
        .collect()
}

pub(super) async fn preparation_fixture_on<P: StreamProvider + 'static>(
    stream: std::sync::Arc<P>,
    seed: &ProcessFixture,
) -> Result<PreparationFixture<P>> {
    let task = native_task_fixture(
        stream,
        seed.files.clone(),
        seed.results.volume().clone(),
        &seed.issuer,
        &seed.scope,
        seed.owner.input_grants.clone(),
    )
    .await?;
    let crate::distributed::WorkPull::Claimed(lease) = task
        .task_host()
        .pull_work(&crate::distributed::Worker {
            id: "preparation-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        })
        .await?
    else {
        return Err(Error::NotFound("native preparation lease".into()));
    };
    task.task_host().start_task(&lease, None).await?;
    let owner = Arc::new(
        task.task_host()
            .journal_owner(
                TaskId::from_bytes([53; 16]),
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    );
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let root = directory.path().join("view");
    let operation = directory.path().join("operation");
    for path in [
        &root,
        &operation,
        &operation.join("target"),
        &operation.join("backup"),
    ] {
        std::fs::create_dir(path).map_err(|error| Error::Storage(error.to_string()))?;
    }
    let options = NativeViewOptions {
        root: root.clone(),
        maximum_volumes: 3,
        work_per_volume: seed.view.manifest().options.work_per_volume,
    };
    let allocation = NativePreparationRoot::witness(
        root,
        operation,
        options.work_per_volume,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map_err(|(error, _)| error)?
    .value;
    let second_generation = seed
        .files
        .resolve(&workspace_ref(
            seed.second_destination.provider().clone(),
            &seed.second_destination.storage_name(&mut None)?,
        )?)
        .await?
        .generation;
    let mut bindings = Vec::new();
    for (volume, path, writable) in [
        (&seed.source, "a-source", false),
        (&seed.destination, "b-destination", true),
    ] {
        let generation = seed
            .view
            .manifest()
            .volumes
            .iter()
            .find(|mapping| &mapping.volume == volume)
            .ok_or_else(|| Error::NotFound("original native preparation mapping".into()))?
            .generation
            .clone();
        bindings.push(PreparationSelection {
            host: seed.files.clone(),
            volume: volume.clone(),
            generation,
            path: path.into(),
            writable,
            verifier: seed.issuer.verifier(&mut None).expect("verifier copy"),
            scope: seed.scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        });
    }
    bindings.push(PreparationSelection {
        host: seed.files.clone(),
        volume: seed.second_destination.clone(),
        generation: second_generation,
        path: "c-second-destination".into(),
        writable: true,
        verifier: seed.issuer.verifier(&mut None).expect("verifier copy"),
        scope: seed.scope.clone(),
        publication: acyclic_fs::PublicationPermit::Unrestricted,
    });
    Ok(PreparationFixture {
        _directory: directory,
        task,
        owner,
        allocation,
        options,
        bindings,
    })
}

#[cfg(feature = "filesystem-local")]
pub(super) async fn reopen_native(
    storage: &std::path::Path,
    seed: &NativeRestart,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<LocalNativeRecovery> {
    let issuer = AuthorityIssuer::new(
        "native-test",
        [63; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "native-test-conversation".into(),
        },
    );
    let files = FilesystemHost::new(
        Fs::local(acyclic_fs::LocalOptions::new(storage.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        &ProviderRef::new(
            "native-test".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?,
        None,
    )?;
    let content = FilesystemContentVerifier::new(
        files.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &seed.scope,
        1_000_000,
        None,
    )?;
    let results = FilesystemContentPublisher::new(
        files.clone(),
        &seed.result_volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &seed.scope,
        1_000_000,
        None,
    )?;
    let reopened = Arc::new(
        acyclic_stream::LocalStream::open(storage.join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let task = native_task_fixture(
        reopened,
        files.clone(),
        seed.result_volume.clone(),
        &issuer,
        &seed.scope,
        seed.scope.capabilities().clone(),
    )
    .await?;
    let owner = Arc::new(
        task.task_host()
            .journal_owner(seed.task, seed.fence.clone())
            .await?,
    );
    let approvals = Arc::new(FilesystemExecutionJournal::new(
        owner.stream(),
        files.clone(),
        &seed.result_volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &crate::core::SchemaRegistry::new(),
        &seed.scope,
        1_000_000,
        None,
    )?);
    let process = Arc::new(
        LocalNativeProcess::recover(
            owner.clone(),
            seed.command,
            &seed.plan,
            task_interaction_id(seed.task, seed.command),
            NativeProcessAuthority {
                verifier: issuer.verifier(&mut None).expect("verifier copy"),
                history_limits: crate::store::HistoryReadLimits {
                    maximum_events: 128,
                    maximum_bytes: 32 * 1024 * 1024,
                },
                content: content.clone(),
                result_content: content.clone(),
                results,
                approvals,
                authority_custody: None,
            },
            work,
        )
        .await?,
    );
    let mut registry = EffectRegistry::default().with_result_resolver(content.clone());
    registry.register(process.clone(), None)?;
    let effects = ConversationEffectHost::new(
        owner.stream(),
        issuer
            .verifier(&mut None)
            .expect("verifier copy")
            .audience()
            .clone(),
        issuer,
        seed.scope.clone(),
        SchemaRegistry::new(),
        content,
        registry,
        None,
        None,
    )?
    .clone_with_native_work(work)?;
    Ok(LocalNativeRecovery {
        files,
        task,
        owner,
        process,
        effects,
    })
}
