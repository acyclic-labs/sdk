use super::*;

pub(super) async fn process_fixture<P: StreamProvider + 'static>(
    stream: std::sync::Arc<P>,
    mode: &str,
    approve: bool,
    work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<ProcessFixture<P>> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    process_fixture_with_filesystems(
        stream,
        directory,
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
    stream: std::sync::Arc<P>,
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
    let files = Arc::new(FilesystemHost::new(filesystem.clone(), provider.clone())?);
    let agent = AgentId::from_bytes([61; 16]);
    let auxiliary_provider = ProviderRef::new(
        "native-test-aux".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let auxiliary_host = Arc::new(FilesystemHost::new(
        auxiliary_filesystem,
        auxiliary_provider.clone(),
    )?);
    let auxiliary = VolumeRef::new(
        auxiliary_provider,
        "auxiliary",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let source = VolumeRef::new(
        provider.clone(),
        "source",
        if mode == "mixed-issued-view" || mode == "review-test" {
            VolumeClass::Project
        } else {
            VolumeClass::AgentPrivate
        },
        if mode == "mixed-issued-view" || mode == "review-test" {
            VolumeOwner::Project("fixture-project".into())
        } else {
            VolumeOwner::Agent(agent)
        },
    )?;
    let destination = VolumeRef::new(
        provider.clone(),
        "destination",
        if mode == "review-test" {
            VolumeClass::Project
        } else {
            VolumeClass::AgentPrivate
        },
        if mode == "review-test" {
            source.owner().clone()
        } else {
            VolumeOwner::Agent(agent)
        },
    )?;
    let second_destination = VolumeRef::new(
        provider.clone(),
        "second-destination",
        if mode == "review-test" {
            VolumeClass::Project
        } else {
            VolumeClass::AgentPrivate
        },
        if mode == "review-test" {
            source.owner().clone()
        } else {
            VolumeOwner::Agent(agent)
        },
    )?;
    let result_volume = VolumeRef::new(
        provider,
        "results",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let extra_sources = (0..if mode == "many-bash" { 16 } else { 0 })
        .map(|index| {
            VolumeRef::new(
                source.provider().clone(),
                format!("extra-{index}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let command = OperationId::from_bytes([62; 16]);
    let approval = task_interaction_id(TaskId::from_bytes([53; 16]), command);
    let conversation = Authority {
        kind: AggregateKind::Conversation,
        id: "native-test-conversation".into(),
    };
    let issuer = AuthorityIssuer::new("native-test", [63; 32], conversation);
    let grants = Capabilities::new(
        [
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "task:spawn:test.effect@1".into(),
            source.capability(VolumeOperation::Read)?,
            auxiliary.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Write)?,
            second_destination.capability(VolumeOperation::Read)?,
            second_destination.capability(VolumeOperation::Write)?,
            result_volume.capability(VolumeOperation::Read)?,
            result_volume.capability(VolumeOperation::Write)?,
            "effect:plan".into(),
            "effect:run".into(),
            "effect:provider:test.native".into(),
            "interaction:open".into(),
            "interaction:resolve".into(),
            capability::interaction_respond(approval),
            "interaction:route".into(),
            "conversation:bind".into(),
        ]
        .into_iter()
        .chain(
            extra_sources
                .iter()
                .map(|volume| volume.capability(VolumeOperation::Read))
                .collect::<Result<Vec<_>>>()?,
        ),
    );
    let scope = issuer
        .issue("owner", &grants, Some(agent), None, &mut None)
        .expect("scope proof");
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
        files.create_volume(&destination).await?;
        files.create_volume(&second_destination).await?;
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
    let task_id = TaskId::from_bytes([53; 16]);
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
    let owner = match original_work {
        Some(work) => task
            .task_host()
            .journal_owner_with_native_work(task_id, &owner_fence, work)
            .await?
            .into_native_shared(work)?,
        None => Arc::new(
            task.task_host()
                .journal_owner(task_id, owner_fence.clone())
                .await?,
        ),
    };
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
    let setup_scope = issuer
        .issue(
            "setup",
            &Capabilities::new(
                [
                    source.capability(VolumeOperation::Write)?,
                    auxiliary.capability(VolumeOperation::Write)?,
                ]
                .into_iter()
                .chain(
                    extra_sources
                        .iter()
                        .map(|volume| volume.capability(VolumeOperation::Write))
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let setup_grant = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &setup_scope,
        &source,
        VolumeOperation::Write,
    )?;
    files
        .put_content(
            &source,
            &setup_grant,
            "input.txt",
            b"pinned input",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("source-setup")?,
        )
        .await?;
    let auxiliary_grant = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &setup_scope,
        &auxiliary,
        VolumeOperation::Write,
    )?;
    auxiliary_host
        .put_content(
            &auxiliary,
            &auxiliary_grant,
            "input.txt",
            b"auxiliary input",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("auxiliary-setup")?,
        )
        .await?;
    let auxiliary_generation = auxiliary_host
        .resolve(&workspace_ref(
            auxiliary.provider().clone(),
            &auxiliary.storage_name()?,
        )?)
        .await?
        .generation;
    let source_generation = files
        .resolve(&workspace_ref(
            source.provider().clone(),
            &source.storage_name()?,
        )?)
        .await?
        .generation;
    if mode == "review-test" {
        let read = ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            &source,
            VolumeOperation::Read,
        )?;
        for (volume, key) in [
            (&destination, "review-fork"),
            (&second_destination, "caller-fork"),
        ] {
            let write = ContentGrant::verify(
                &issuer.verifier(&mut None).expect("verifier copy"),
                &scope,
                volume,
                VolumeOperation::Write,
            )?;
            files
                .fork_volume(
                    &source,
                    &read,
                    &source_generation,
                    volume,
                    &write,
                    None,
                    &IdempotencyKey::new(key)?,
                )
                .await?;
        }
    }
    let destination_generation = files
        .resolve(&workspace_ref(
            destination.provider().clone(),
            &destination.storage_name()?,
        )?)
        .await?
        .generation;
    let second_generation = files
        .resolve(&workspace_ref(
            second_destination.provider().clone(),
            &second_destination.storage_name()?,
        )?)
        .await?
        .generation;
    let root = directory.path().join("view");
    std::fs::create_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    let work = acyclic_fs::performance::work_budget(10_000_000, 10_000_000);
    let operation_directory = directory.path().join("preparation");
    for path in [
        &operation_directory,
        &operation_directory.join("target"),
        &operation_directory.join("backup"),
    ] {
        std::fs::create_dir(path).map_err(|error| Error::Storage(error.to_string()))?;
    }
    let mut bindings = vec![
        PreparationSelection {
            host: files.clone(),
            volume: source.clone(),
            generation: source_generation,
            path: "src".into(),
            writable: false,
            verifier: issuer.verifier(&mut None).expect("verifier copy"),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
        PreparationSelection {
            host: files.clone(),
            volume: destination.clone(),
            generation: destination_generation,
            path: "dst".into(),
            writable: true,
            verifier: issuer.verifier(&mut None).expect("verifier copy"),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
        PreparationSelection {
            host: auxiliary_host,
            volume: auxiliary,
            generation: auxiliary_generation,
            path: "aux".into(),
            writable: false,
            verifier: issuer.verifier(&mut None).expect("verifier copy"),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
    ];
    if mode == "partial-publication" {
        bindings.push(PreparationSelection {
            host: files.clone(),
            volume: second_destination.clone(),
            generation: second_generation,
            path: "second-dst".into(),
            writable: true,
            verifier: issuer.verifier(&mut None).expect("verifier copy"),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        });
    }
    for (index, volume) in extra_sources.into_iter().enumerate() {
        let grant = ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &setup_scope,
            &volume,
            VolumeOperation::Write,
        )?;
        files
            .put_content(
                &volume,
                &grant,
                "input.txt",
                b"extra input",
                "text/plain",
                "input.txt",
                128,
                &IdempotencyKey::new(format!("extra-source-{index}"))?,
            )
            .await?;
        let generation = files
            .resolve(&workspace_ref(
                volume.provider().clone(),
                &volume.storage_name()?,
            )?)
            .await?
            .generation;
        bindings.push(PreparationSelection {
            host: files.clone(),
            volume,
            generation,
            path: format!("extra-{index}"),
            writable: false,
            verifier: issuer.verifier(&mut None).expect("verifier copy"),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        });
    }
    let maximum_volumes =
        u32::try_from(bindings.len()).map_err(|error| Error::Invalid(error.to_string()))?;
    let options = NativeViewOptions {
        root,
        maximum_volumes,
        work_per_volume: work,
    };
    let borrowed = borrow_preparation_bindings(&bindings);
    let mut lazy_mount = None;
    let mut lazy_work = None;
    let mut project_policy = None;
    let mut project_generation = None;
    let cancellation = acyclic_fs::CancellationToken::new();
    let fallback_work;
    let work = match original_work {
        Some(work) => work,
        None => {
            fallback_work = test_work(
                options.budget_for_volume_count(bindings.len())?,
                &cancellation,
            )?;
            &fallback_work
        }
    };
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
                .mount_with_work(
                    &path,
                    acyclic_fs::MountOptions::read_only()
                        .publication(acyclic_fs::MountPublication::Manual),
                    Some(callback),
                    &volume_work,
                    volume_work.cancellation(),
                    Some(&volume_work),
                )
                .await?;
            mount.retain_payload_allocation(&mut allocation)?;
            let mount = Arc::new(mount);
            let snapshot = mount
                .snapshot_with_work(&volume_work, volume_work.cancellation())
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
            project_generation = Some(crate::resources::GenerationRef::new_with_native_work(
                borrowed[0].generation.as_resource().provider(),
                borrowed[0].generation.as_resource().key(),
                borrowed[0].generation.as_resource().version(),
                work,
            )?);
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
        view.into_arc_with_native_work(work)?
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
    let content = Arc::new(FilesystemContentVerifier::new(
        files.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        1_000_000,
        None,
    )?);
    let results = Arc::new(FilesystemContentPublisher::new(
        files.clone(),
        &result_volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        1_000_000,
        None,
    )?);
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
        &issuer.verifier(&mut None).expect("verifier copy"),
        &crate::core::SchemaRegistry::new(),
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
                request.approval_digest(owner.task_binding().0, command)?,
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
        volume,
        issuer.verifier(&mut None).expect("verifier copy"),
        scope.clone(),
        runtime_scope.clone(),
        tasks,
        machines,
        crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        },
        1,
        1_000_000,
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
            &seed.second_destination.storage_name()?,
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
) -> Result<LocalNativeRecovery> {
    let issuer = AuthorityIssuer::new(
        "native-test",
        [63; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "native-test-conversation".into(),
        },
    );
    let files = Arc::new(FilesystemHost::new(
        Fs::local(acyclic_fs::LocalOptions::new(storage.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        ProviderRef::new(
            "native-test".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?,
    )?);
    let content = Arc::new(FilesystemContentVerifier::new(
        files.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &seed.scope,
        1_000_000,
        None,
    )?);
    let results = Arc::new(FilesystemContentPublisher::new(
        files.clone(),
        &seed.result_volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &seed.scope,
        1_000_000,
        None,
    )?);
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
            seed.plan.clone(),
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
        )
        .await?,
    );
    let mut registry = EffectRegistry::default().with_result_resolver(content.clone());
    registry.register(process.clone())?;
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
    )?;
    Ok(LocalNativeRecovery {
        files,
        task,
        owner,
        process,
        effects,
    })
}
