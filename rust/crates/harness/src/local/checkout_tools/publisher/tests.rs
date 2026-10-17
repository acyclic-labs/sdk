use super::*;

struct Fixture {
    files: Arc<LocalFilesystem>,
    project: VolumeRef,
    verifier: AuthorityVerifier,
    signed: Scope,
    task_files: Arc<crate::task_volume::TaskFiles<LocalStream>>,
    workspace: Option<Arc<LocalWorkspace>>,
    _state: tempfile::TempDir,
}

fn temporary_directory() -> Result<tempfile::TempDir> {
    // Keep real native I/O and durability calls; Linux's memory-backed temporary
    // directory avoids making logical publication tests depend on disk latency.
    #[cfg(target_os = "linux")]
    if Path::new("/dev/shm").is_dir() {
        return tempfile::tempdir_in("/dev/shm").map_err(storage);
    }
    tempfile::tempdir().map_err(storage)
}

async fn fixture() -> Result<(tempfile::TempDir, Fixture, CheckoutPublisher)> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    acyclic_fs::initialize_native_mount_services().map_err(storage)?;
    let directory = temporary_directory()?;
    let state = temporary_directory()?;
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        state.path().join("filesystem"),
    ))
    .await
    .map_err(storage)?;
    let provider = crate::resources::ProviderRef::new(
        "publisher".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let project = VolumeRef::new(
        provider.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("publisher".into()),
        &mut None,
    )?;
    let agent = crate::AgentId::from_bytes([1; 16]);
    let private = VolumeRef::new(
        provider.clone(),
        "task-files".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let identity = work.root_identity(directory.path())?.to_bytes();
    let workspace = Arc::new(
        workspace::open(
            &filesystem,
            &project,
            &private,
            directory.path(),
            state.path(),
            Some(identity),
            acyclic_fs::OperationId::new(),
            false,
            false,
            acyclic_fs::LocalCoreStateStore::open_shared_owner(state.path().join("workspace"))
                .map_err(storage)?,
            None,
        )
        .await?,
    );
    let files = FilesystemHost::new(filesystem, &provider, None)?;
    let issuer = AuthorityIssuer::new(
        "publisher",
        [17; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "publisher".into(),
        },
    );
    let verifier = issuer.verifier(&mut None)?;
    let signed = issuer.issue(
        "publisher",
        &Capabilities::new([
            project.capability(VolumeOperation::Read, &mut None)?,
            project.capability(VolumeOperation::Write, &mut None)?,
            private.capability(VolumeOperation::Read, &mut None)?,
            private.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    let stream = Arc::new(
        LocalStream::open(state.path().join("stream"), Default::default())
            .await
            .map_err(storage)?,
    );
    let task_files = Arc::new(
        crate::task_volume::TaskFiles::open(stream, &private, &verifier, &signed, 65536).await?,
    );
    let tools = Arc::new(
        CheckoutTools::new(
            workspace.clone(),
            &project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &signed,
            task_files.clone(),
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(tools, files.clone(), &project, &verifier, 65536);
    Ok((
        directory,
        Fixture {
            files,
            project,
            verifier,
            signed,
            task_files,
            workspace: Some(workspace),
            _state: state,
        },
        publisher,
    ))
}

#[tokio::test]
async fn task_mount_options_require_and_retain_the_authenticated_volume_owner() -> Result<()> {
    let (_directory, mut fixture, publisher) = fixture().await?;
    drop(publisher);
    let mut workspace = Arc::try_unwrap(fixture.workspace.take().expect("fixture workspace"))
        .map_err(|_| Error::Conflict("fixture workspace is still shared".into()))?;
    assert!(matches!(
        workspace.mount_options(workspace.view().workspace()).await,
        Err(Error::FilesystemAccessConfiguration("native mount owner"))
    ));
    let issuer = AuthorityIssuer::new("publisher", [17; 32], fixture.verifier.audience().clone());
    let agent = fixture.signed.agent().expect("fixture agent");
    workspace.mount_authority = Some((Arc::new(issuer), agent));
    let options = workspace
        .mount_options(workspace.view().workspace())
        .await?;
    let view = options
        .require_access_view()
        .map_err(storage)?
        .current()
        .map_err(storage)?;
    assert_eq!(
        view.volume(),
        uuid::Uuid::from_bytes(workspace.view().workspace().id().volume_id().into_bytes())
            .to_string()
    );
    assert_eq!(view.subject().1, agent.to_string());
    assert!(
        view.decide(
            acyclic_fs::access::Action::Read,
            "file",
            crate::filesystem::access::access_now()
        )
        .allowed
    );
    Ok(())
}

#[tokio::test]
async fn file_publication_materializes_its_exact_committed_bytes() -> Result<()> {
    let (directory, harness, publisher) = fixture().await?;
    let (file, _) = publisher
        .stage_once(
            OperationId::new(),
            "published.txt",
            b"exact bytes",
            "text/plain",
            "published.txt",
            None,
            None,
        )?
        .await?;
    assert_eq!(file.volume(), &harness.project);
    assert_eq!(
        std::fs::read(directory.path().join("published.txt")).map_err(storage)?,
        b"exact bytes"
    );
    assert!(!directory.path().join(".system").exists());
    Ok(())
}

#[tokio::test]
async fn open_reads_checkout_selection_once_when_no_publication_is_pending() -> Result<()> {
    let (_directory, harness, publisher) = fixture().await?;
    let workspace = publisher.tools.workspace.clone();
    let before = workspace
        .selection_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    CheckoutTools::new(
        workspace.clone(),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        &harness.signed,
        Arc::clone(&harness.task_files),
    )
    .await?;
    assert_eq!(
        workspace
            .selection_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        before,
        "open reloaded Git state after selecting the checkout"
    );
    Ok(())
}

#[tokio::test]
async fn committed_uploads_recover_checkout_before_capture_and_never_reapply_completed_edits()
-> Result<()> {
    let (directory, harness, publisher) = fixture().await?;
    let view = harness.workspace.as_ref().unwrap().view();
    let writer = publisher.writer_for(&harness.project, view.workspace())?;
    let operation = OperationId::new();
    // Crash boundary: the actual volume write commits, but no native
    // preparation journal or checkout edit has happened.
    let (original, _) = writer
        .stage_once(
            operation,
            "once.txt",
            b"once",
            "text/plain",
            "once.txt",
            None,
            None,
        )?
        .await?;
    assert!(!directory.path().join("once.txt").exists());
    // Recovery normally enters through observe_staged_once, before executing
    // the tool again. It must repair the checkout as well as return its ref.
    assert_eq!(
        publisher
            .observe_staged_once(operation, 1024, None, None)?
            .await?
            .0
            .as_ref(),
        Some(&original)
    );
    assert_eq!(
        std::fs::read(directory.path().join("once.txt")).map_err(storage)?,
        b"once"
    );
    assert_eq!(
        publisher
            .stage_once(
                operation,
                "once.txt",
                b"once",
                "text/plain",
                "once.txt",
                None,
                None,
            )?
            .await?
            .0,
        original
    );

    let operation = OperationId::new();
    let (original, _) = writer
        .stage(
            operation,
            "path.txt",
            b"path",
            "text/plain",
            "path.txt",
            None,
            None,
        )?
        .await?;
    assert!(!directory.path().join("path.txt").exists());
    assert_eq!(
        publisher
            .stage(
                operation,
                "path.txt",
                b"path",
                "text/plain",
                "path.txt",
                None,
                None,
            )?
            .await?
            .0,
        original
    );
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"path"
    );

    let operation = OperationId::new();
    let (replacement, _) = writer
        .stage_at(operation, &original, b"replacement", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"path"
    );
    assert_eq!(
        publisher
            .stage_at(operation, &original, b"replacement", None, None)?
            .await?
            .0,
        replacement
    );
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"replacement"
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let before = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    std::fs::write(directory.path().join("path.txt"), b"later user edit").map_err(storage)?;
    assert_eq!(
        publisher
            .stage_at(operation, &original, b"replacement", None, None)?
            .await?
            .0,
        replacement
    );
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"later user edit"
    );
    // Git recovery may enter with the already-committed target as both
    // heads. It must use the saved source and retain later checkout edits too.
    let head = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    crate::tools::git::publish_checkout_transition(
        directory.path(),
        publisher.tools.workspace.identity,
        &publisher.tools.workspace.store,
        publication_operation(operation, original.path()),
        head.clone(),
        head,
        &|| std::future::ready(Ok(())),
    )
    .await?;
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"later user edit"
    );
    let after = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    assert_eq!(
        before, after,
        "retry captured the missing native write or authored another upload"
    );
    Ok(())
}

#[tokio::test]
async fn one_operation_publishes_each_path_and_retries_without_overwriting_edits() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let mut files = Vec::new();
    for (path, bytes) in [
        ("first.txt", b"first".as_slice()),
        ("second.txt", b"second".as_slice()),
    ] {
        let (file, _) = publisher
            .stage(operation, path, bytes, "text/plain", path, None, None)?
            .await?;
        assert_eq!(
            std::fs::read(directory.path().join(path)).map_err(storage)?,
            bytes
        );
        files.push((file, bytes));
    }
    for (file, bytes) in files {
        std::fs::write(directory.path().join(file.path()), "later edit").map_err(storage)?;
        let (retried, _) = publisher
            .stage(
                operation,
                file.path(),
                bytes,
                "text/plain",
                file.display_name(),
                None,
                None,
            )?
            .await?;
        assert_eq!(retried, file);
        assert_eq!(
            std::fs::read(directory.path().join(file.path())).map_err(storage)?,
            b"later edit"
        );
    }
    Ok(())
}

#[test]
fn checkout_publication_preserves_caller_ceilings() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (_directory, harness, publisher) = runtime.block_on(fixture()).expect("publisher fixture");
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 8,
        source_file: Some(file!()),
        ..Default::default()
    });
    runner
        .run(
            &("file-[a-z]{1,8}\\.txt", proptest::bool::ANY),
            |(path, allowed)| {
                runtime
                    .block_on(async {
                        let ceiling = if allowed {
                            harness.signed.capabilities().clone()
                        } else {
                            crate::Capabilities::default()
                        };
                        let operation = OperationId::new();
                        let (source, _) = publisher
                            .stage_once(
                                operation,
                                &path,
                                b"original",
                                "text/plain",
                                &path,
                                None,
                                None,
                            )?
                            .await?;
                        // Capture a real unrelated edit before obtaining the immutable
                        // replacement reference. stage_at must reject the earlier head.
                        std::fs::write(
                            _directory
                                .path()
                                .join(format!("user-sibling-{operation}.txt")),
                            b"user edit",
                        )
                        .map_err(storage)?;
                        let fresh = serialized(&publisher.tools.state, async |mut state| {
                            let State {
                                active, watcher, ..
                            } = &mut *state;
                            let view = active
                                .as_ref()
                                .unwrap_or_else(|| publisher.tools.workspace.view());
                            publisher
                                .tools
                                .capture(
                                    (&publisher.tools.workspace, view),
                                    watcher,
                                    OperationId::new(),
                                    0,
                                    PublicationPermit::Unrestricted,
                                    None,
                                )
                                .await?;
                            let host = harness
                                .files
                                .for_workspace(&harness.project, view.workspace().clone())?;
                            let grant = crate::conversation::ContentGrant::verify_directory_read(
                                &harness.verifier,
                                &harness.signed,
                                &harness.project,
                                "",
                                &mut None,
                            )?;
                            let (fresh, bytes) = host
                                .read_private_path(
                                    &harness.project,
                                    &grant,
                                    source.path(),
                                    None,
                                    1024,
                                    None,
                                    None,
                                )
                                .await?;
                            assert_eq!(bytes.as_ref(), b"original");
                            Ok::<_, Error>(fresh)
                        })
                        .await?;
                        assert_ne!(source.version(), fresh.version());
                        assert!(matches!(
                            publisher
                                .stage_at(OperationId::new(), &source, b"stale", None, None)?
                                .await,
                            Err(Error::Conflict(_))
                        ));
                        let replacement = publisher
                            .stage_at(
                                OperationId::new(),
                                &fresh,
                                b"replaced",
                                Some(&ceiling),
                                None,
                            )?
                            .await;
                        assert_eq!(
                            replacement.is_ok(),
                            allowed,
                            "{:?}",
                            replacement.as_ref().err()
                        );
                        let observed = publisher
                            .observe_staged_once(operation, 1024, Some(&ceiling), None)?
                            .await;
                        assert_eq!(observed.is_ok(), allowed, "{:?}", observed.as_ref().err());
                        for once in [false, true] {
                            let target = format!("{once}-{path}");
                            let operation = OperationId::new();
                            let result = if once {
                                publisher
                                    .stage_once(
                                        operation,
                                        &target,
                                        b"new",
                                        "text/plain",
                                        &target,
                                        Some(&ceiling),
                                        None,
                                    )?
                                    .await
                            } else {
                                publisher
                                    .stage(
                                        operation,
                                        &target,
                                        b"new",
                                        "text/plain",
                                        &target,
                                        Some(&ceiling),
                                        None,
                                    )?
                                    .await
                            };
                            assert_eq!(result.is_ok(), allowed, "{:?}", result.as_ref().err());
                            if allowed {
                                // A committed reply still needs the current caller's ceiling.
                                let empty = crate::Capabilities::default();
                                let replay = if once {
                                    publisher
                                        .stage_once(
                                            operation,
                                            &target,
                                            b"new",
                                            "text/plain",
                                            &target,
                                            Some(&empty),
                                            None,
                                        )?
                                        .await
                                } else {
                                    publisher
                                        .stage(
                                            operation,
                                            &target,
                                            b"new",
                                            "text/plain",
                                            &target,
                                            Some(&empty),
                                            None,
                                        )?
                                        .await
                                };
                                assert!(
                                    replay.is_err(),
                                    "cached publication bypassed the caller ceiling"
                                );
                            }
                        }
                        Ok::<_, crate::Error>(())
                    })
                    .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))
            },
        )
        .expect("caller ceiling property");
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn publication_identity_binds_operation_and_path(
        operation in proptest::prelude::any::<[u8;16]>(),
        path in "[a-z]{1,16}(/[a-z]{1,16}){0,3}",
    ) {
        let original = OperationId::from_bytes(operation);
        let first = publication_operation(original, &path);
        proptest::prop_assert_eq!(first, publication_operation(original, &path));
        proptest::prop_assert_ne!(first, publication_operation(original, &format!("{path}x")));
        let mut other = operation; other[0] ^= 1;
        proptest::prop_assert_ne!(first, publication_operation(OperationId::from_bytes(other), &path));
    }
}

#[tokio::test]
async fn raw_checkout_commands_reject_restrictive_policy_and_caller_ceiling() -> Result<()> {
    use acyclic_fs::access::{Action, Capability, Generations, Grant, Policy, ResourcePattern};
    let (directory, fixture, publisher) = fixture().await?;
    std::fs::create_dir(directory.path().join("secrets")).map_err(storage)?;
    std::fs::write(
        directory.path().join("secrets/key"),
        b"must not be disclosed",
    )
    .map_err(storage)?;
    let tools = publisher.tools.clone();
    let all = Capabilities::new(
        fixture
            .signed
            .capabilities()
            .iter()
            .chain(["tool:call:bash", "tool:call:fs"]),
    );
    super::super::access::require_mount_access(
        tools.workspace.view().workspace(),
        &tools.workspace.mounts,
        &tools.signed,
        &all,
        "local bash",
    )
    .await?;
    let restricted = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: "public/**".into(),
            },
        }]],
        expires_at: None,
    };
    let label = restricted
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let restricted = Capabilities::new(all.iter().chain([label.as_str()]));
    async fn execute(tools: Arc<CheckoutTools>, grants: Capabilities) -> Result<()> {
        let mut bindings = crate::runtime::Bindings::local();
        bindings.scope = RuntimeScope::new(grants, crate::conversation::Limits::default())?;
        bindings.tasks.register(crate::runtime::TaskDefinition::live("raw-policy", "1", move |task, name: String| {
            let tools = tools.clone();
            async move {
                let context = ToolContext::new(Arc::new(task), OperationId::new(), "policy-call", None)?;
                let invocation = ToolInvocation { operation_id: context.operation_id(), call_id: context.call_id().into(),
                    name: name.clone(), arguments: serde_json::json!({"command": if name == "bash" { "cat secrets/key" } else { "grep secret" }}) };
                Ok::<_, Error>(matches!(tools.execute(invocation, Some(context)).await,
                    Err(Error::FilesystemAccessConfiguration(_))))
            }
        })?)?;
        let runtime = bindings.build(None)?;
        let definition = runtime.task::<String, bool>("raw-policy@1")?;
        for name in ["bash", "fs"] {
            assert_eq!(
                runtime
                    .spawn(&definition, name.into())
                    .await?
                    .result()
                    .await?,
                crate::Outcome::Succeeded(true)
            );
        }
        Ok(())
    }
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let before = tools
        .workspace
        .view()
        .workspace()
        .head(budget, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    let denied = publisher
        .stage_once(
            OperationId::new(),
            "public/new.txt",
            b"new",
            "text/plain",
            "new",
            Some(&restricted),
            None,
        )?
        .await;
    assert!(matches!(
        denied,
        Err(Error::FilesystemAccessConfiguration(
            "local checkout publication"
        ))
    ));
    drop(denied);
    assert_eq!(
        before,
        tools
            .workspace
            .view()
            .workspace()
            .head(budget, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        "denied publication imported unrelated physical edits"
    );
    assert!(matches!(
        publisher
            .observe_staged_once(OperationId::new(), 1024, Some(&restricted), None)?
            .await,
        Err(Error::FilesystemAccessConfiguration(
            "local checkout publication"
        ))
    ));
    execute(tools.clone(), restricted).await?;
    let mut policy: Policy =
        serde_json::from_slice(crate::filesystem::access::INITIAL_POLICY).map_err(storage)?;
    policy.version += 1;
    let mut deny = policy.layers[0][0].clone();
    deny.id = "deny-secret".into();
    deny.effect = acyclic_fs::access::Effect::Deny;
    deny.actions = [Action::Read].into_iter().collect();
    deny.resource.path = "secrets/**".into();
    policy.layers[0].push(deny);
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut checkout = tools
        .workspace
        .view()
        .workspace()
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            budget,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(&policy).map_err(storage)?,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            budget,
            &cancellation,
        )
        .await
        .map_err(storage)?;
    checkout
        .commit(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| {
                std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: Default::default(),
                    },
                ))
            },
        )
        .await
        .map_err(storage)?;
    let before = tools
        .workspace
        .view()
        .workspace()
        .head(budget, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    assert!(matches!(
        publisher
            .stage_once(
                OperationId::new(),
                "public/new.txt",
                b"new",
                "text/plain",
                "new",
                None,
                None
            )?
            .await,
        Err(Error::FilesystemAccessConfiguration(
            "local checkout publication"
        ))
    ));
    assert_eq!(
        before,
        tools
            .workspace
            .view()
            .workspace()
            .head(budget, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id()
    );
    execute(tools, all).await?;
    assert_eq!(
        std::fs::read(directory.path().join("secrets/key")).map_err(storage)?,
        b"must not be disclosed"
    );
    Ok(())
}

#[tokio::test]
async fn executor_bindings_share_one_initial_checkout_selection() -> Result<()> {
    let (_directory, harness, publisher) = fixture().await?;
    let workspace = publisher.tools.workspace.clone();
    let before = workspace
        .selection_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(before, 1, "initial binding must select the checkout once");
    CheckoutTools::new(
        workspace.clone(),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        &harness.signed,
        Arc::clone(&harness.task_files),
    )
    .await?;
    assert_eq!(
        workspace
            .selection_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        before,
        "another binding reloaded the already selected checkout"
    );
    Ok(())
}
