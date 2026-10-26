use super::*;

mod admission_cost;
mod observed_cost;

thread_local! {
    /// Counterfactual: reread and hash even an identical authenticated payload.
    pub(super) static REREAD_MATCHING_PAYLOAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Stops a publication after its physical write, before its capture.
    pub(super) static KILL_BEFORE_CAPTURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Negative control: a replay never captures, as before the fix.
    pub(super) static REPLAY_SKIPS_CAPTURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct Fixture {
    _runtime: AuthorityRoot,
    files: Arc<LocalFilesystem>,
    project: VolumeRef,
    verifier: AuthorityVerifier,
    signed: Scope,
    workspace: Option<Arc<LocalWorkspace>>,
    _state: tempfile::TempDir,
}

impl Fixture {
    fn into_state(self) -> tempfile::TempDir {
        self._state
    }
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
    let directory = temporary_directory()?;
    let (fixture, publisher) = open_fixture(directory.path(), temporary_directory()?).await?;
    Ok((directory, fixture, publisher))
}

async fn open_fixture(
    directory: &Path,
    state: tempfile::TempDir,
) -> Result<(Fixture, CheckoutPublisher)> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    acyclic_fs::initialize_native_mount_services().map_err(storage)?;
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
    let workspace = Arc::new(
        workspace::open(
            &filesystem,
            &workspace::Attach {
                volume: &project,
                scratch: &private,
                directory: state.path(),
                operation: acyclic_fs::OperationId::new(),
                native_mounts: false,
                authority: None,
            },
            acyclic_fs::hold_root(directory).map_err(storage)?,
            false,
            workspace::OwnerState {
                store: acyclic_fs::LocalCoreStateStore::open_shared_owner(
                    state.path().join("workspace"),
                )
                .map_err(storage)?,
                processes: None,
            },
        )
        .await?,
    );
    let files = FilesystemHost::new(filesystem, &provider, None)?;
    let issuer = AuthorityRoot::new(
        "publisher",
        [17; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "publisher".into(),
        },
    );
    let verifier = issuer.verifier(&mut None)?;
    let signed = issuer.bootstrap_scope(
        "publisher",
        &Capabilities::new([
            project.capability(VolumeOperation::Read, &mut None)?,
            project.capability(VolumeOperation::Write, &mut None)?,
            private.capability(VolumeOperation::Read, &mut None)?,
            private.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        &mut None,
    )?;
    let tools = Arc::new(
        CheckoutTools::new(
            workspace.clone(),
            &project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &signed,
            &verifier,
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(tools, files.clone(), &project, &verifier, 65536);
    Ok((
        Fixture {
            _runtime: issuer,
            files,
            project,
            verifier,
            signed,
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
    let issuer = AuthorityRoot::new("publisher", [17; 32], fixture.verifier.audience().clone());
    let agent = fixture.signed.agent().expect("fixture agent");
    workspace.mount_authority = Some(crate::filesystem::NativeMountAuthority::new(
        &issuer, agent,
    )?);
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
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(storage)?
                .as_secs()
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
            crate::conversation::ContentAccess::default(),
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
        &harness.verifier,
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

/// A replay publishes the staged file again: the staged reference itself,
/// or, once the replay finished the write's pending capture, the reference
/// at that capture, which holds exactly the same bytes. Later edits name the
/// reference the replay returned, as an agent would.
fn assert_republished(replayed: &FileRef, staged: &FileRef) -> FileRef {
    assert_eq!(
        (replayed.volume(), replayed.path(), replayed.descriptor()),
        (staged.volume(), staged.path(), staged.descriptor()),
    );
    replayed.clone()
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
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert!(!directory.path().join("once.txt").exists());
    // Recovery normally enters through observe_staged_once, before executing
    // the tool again. It must repair the checkout as well as return its ref,
    // finishing the write's pending capture as a replayed write would.
    let recovered = publisher
        .observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .0
        .ok_or_else(|| Error::Invalid("the committed upload was not recovered".into()))?;
    let original = assert_republished(&recovered, &original);
    assert_eq!(
        std::fs::read(directory.path().join("once.txt")).map_err(storage)?,
        b"once"
    );
    assert_republished(
        &publisher
            .stage_once(
                operation,
                "once.txt",
                b"once",
                "text/plain",
                "once.txt",
                crate::conversation::ContentAccess::default(),
            )?
            .await?
            .0,
        &original,
    );

    let operation = OperationId::new();
    let (original, _) = writer
        .stage(
            operation,
            "path.txt",
            b"path",
            "text/plain",
            "path.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert!(!directory.path().join("path.txt").exists());
    let original = assert_republished(
        &publisher
            .stage(
                operation,
                "path.txt",
                b"path",
                "text/plain",
                "path.txt",
                crate::conversation::ContentAccess::default(),
            )?
            .await?
            .0,
        &original,
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
    assert_republished(
        &publisher
            .stage_at(operation, &original, b"replacement", None, None)?
            .await?
            .0,
        &replacement,
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
    let capture_key = acyclic_fs::IdempotencyKey::from_bytes(
        capture::capture_operation(write_capture(operation, original.path()), 1).into_bytes(),
    );
    let captured = view
        .workspace()
        .operation_generation(
            capture_key,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value
        .map(|generation| generation.id());
    assert_eq!(
        captured,
        Some(before),
        "the write's capture must have a durable receipt"
    );
    std::fs::write(directory.path().join("path.txt"), b"later user edit").map_err(storage)?;
    assert_republished(
        &publisher
            .stage_at(operation, &original, b"replacement", None, None)?
            .await?
            .0,
        &replacement,
    );
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"later user edit"
    );
    // Git recovery may enter with the already-committed target as both
    // heads. It must use the saved source and retain later checkout edits too.
    // The replay above already captured the write, so the target is the
    // staged generation below the head.
    let head = view
        .workspace()
        .generation(
            publisher.staged_generation(&replacement)?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
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
async fn a_clean_owed_capture_seals_its_completion_before_a_later_edit() -> Result<()> {
    let (directory, harness, publisher) = fixture().await?;
    let (key, captured_id) = {
        let physical = &*publisher.tools.workspace;
        let view = physical.view();
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget::UNBOUNDED;
        let before = view
            .workspace()
            .head(budget, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let operation = OperationId::new();
        let mut cursor = None;
        let key = acyclic_fs::IdempotencyKey::from_bytes(
            capture::capture_operation(operation, 1).into_bytes(),
        );
        // Negative control: the ordinary clean-capture path used before this fix
        // completes without a receipt. It cannot acknowledge an owed effect.
        publisher
            .tools
            .capture(
                (physical, view),
                &mut cursor,
                operation,
                1,
                PublicationPermit::Unrestricted,
            )?
            .await?;
        assert!(
            view.workspace()
                .operation_generation(key, budget, &cancellation)
                .await
                .map_err(storage)?
                .value
                .is_none()
        );
        let owed = (
            acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
            acyclic_fs::GitGenerationRef {
                workspace_id: view.workspace().id(),
                generation: before.id(),
            },
        );
        publisher
            .tools
            .owe_write_capture((physical, view), &mut cursor, owed)
            .await?;
        publisher
            .tools
            .finish_owed_write(
                (physical, view),
                &mut cursor,
                PublicationPermit::Unrestricted,
            )?
            .await?;
        let captured = view
            .workspace()
            .operation_generation(key, budget, &cancellation)
            .await
            .map_err(storage)?
            .value
            .ok_or_else(|| Error::Invalid("clean capture lost its completion receipt".into()))?;
        // A clean completion records its operation without changing tree identity.
        // Recovery must consult the receipt even while the staged head still holds.
        assert_eq!(captured.id(), before.id());
        std::fs::write(directory.path().join("later.txt"), b"user edit").map_err(storage)?;
        publisher
            .tools
            .owe_write_capture((physical, view), &mut cursor, owed)
            .await?;
        (key, captured.id())
    };
    // Retain only immutable fixture paths. The recovery below reconstructs the
    // filesystem, journal owner, checkout selection and watcher from disk.
    drop(publisher);
    let (_harness, publisher) = open_fixture(directory.path(), harness.into_state()).await?;
    let physical = &*publisher.tools.workspace;
    let view = physical.view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    assert_eq!(
        view.workspace()
            .operation_generation(key, budget, &cancellation)
            .await
            .map_err(storage)?
            .value
            .map(|generation| generation.id()),
        Some(captured_id)
    );
    let mut cursor = None;
    publisher
        .tools
        .finish_owed_write(
            (physical, view),
            &mut cursor,
            PublicationPermit::Unrestricted,
        )?
        .await?;
    assert!(reconciliation::owed_write(physical).await?.is_none());
    assert_eq!(
        view.workspace()
            .head(budget, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        captured_id
    );
    assert_eq!(
        std::fs::read(directory.path().join("later.txt")).map_err(storage)?,
        b"user edit"
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
            .stage(
                operation,
                path,
                bytes,
                "text/plain",
                path,
                crate::conversation::ContentAccess::default(),
            )?
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
                crate::conversation::ContentAccess::default(),
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
                                crate::conversation::ContentAccess::default(),
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
                                )?
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
                                    crate::conversation::ContentAccess::default(),
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
                            .observe_staged_once(
                                operation,
                                1024,
                                crate::conversation::ContentAccess {
                                    ceiling: Some(&ceiling),
                                    io: None,
                                },
                            )?
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
                                        crate::conversation::ContentAccess {
                                            ceiling: Some(&ceiling),
                                            io: None,
                                        },
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
                                        crate::conversation::ContentAccess {
                                            ceiling: Some(&ceiling),
                                            io: None,
                                        },
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
                                            crate::conversation::ContentAccess {
                                                ceiling: Some(&empty),
                                                io: None,
                                            },
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
                                            crate::conversation::ContentAccess {
                                                ceiling: Some(&empty),
                                                io: None,
                                            },
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
    // Real shell negative control: the old raw checkout path can write the
    // forbidden file. The product invocation below must refuse before dispatch.
    #[cfg(unix)]
    {
        let status = std::process::Command::new("bash")
            .args(["-c", "echo compromised > secrets/key"])
            .current_dir(directory.path())
            .status()
            .map_err(storage)?;
        assert!(status.success());
        assert_eq!(
            std::fs::read(directory.path().join("secrets/key")).map_err(storage)?,
            b"compromised\n"
        );
        std::fs::write(
            directory.path().join("secrets/key"),
            b"must not be disclosed",
        )
        .map_err(storage)?;
    }
    let tools = publisher.tools.clone();
    let shell_grant = format!("tool:call:{}", crate::tools::shell::PLATFORM.name);
    let all = Capabilities::new(
        fixture
            .signed
            .capabilities()
            .iter()
            .chain([shell_grant.as_str(), "tool:call:fs"]),
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
    async fn execute(
        tools: Arc<CheckoutTools>,
        grants: Capabilities,
        path_only: bool,
    ) -> Result<()> {
        let mut bindings = crate::runtime::Bindings::local();
        bindings.scope = RuntimeScope::new(grants, crate::conversation::Limits::default())?;
        bindings.tasks.register(crate::runtime::TaskDefinition::live("raw-policy", "1", move |task, name: String| {
            let tools = tools.clone();
            async move {
                let context = ToolContext::new(Arc::new(task), OperationId::new(), "policy-call")?;
                let invocation = ToolInvocation { operation_id: context.operation_id(), call_id: context.call_id().into(),
                    name: name.clone(), arguments: serde_json::json!({"command": if name == "fs" { "grep secret" } else { "echo compromised > secrets/key" }}) };
                match tools.execute(invocation, Some(context)).await {
                    Err(Error::FilesystemAccessConfiguration(_)) => Ok::<_, Error>(()),
                    // Without whole-volume labels, selection requires an
                    // authenticated child allocation. This ordinary task has
                    // none, so it must be refused before checkout dispatch.
                    Err(Error::Unauthorized(reason)) if path_only && reason == "child workspace has no original owner" => Ok(()),
                    result => Err(Error::Invalid(format!("{name} must reject filesystem access configuration: {result:?}"))),
                }
            }
        })?)?;
        let runtime = bindings.build(None)?;
        let definition = runtime.task::<String, ()>("raw-policy@1")?;
        for name in [crate::tools::shell::PLATFORM.name, "fs"] {
            assert_eq!(
                runtime
                    .spawn(&definition, name.into())
                    .await?
                    .result()
                    .await?,
                crate::Outcome::Succeeded(())
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
            crate::conversation::ContentAccess {
                ceiling: Some(&restricted),
                io: None,
            },
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
            .observe_staged_once(
                OperationId::new(),
                1024,
                crate::conversation::ContentAccess {
                    ceiling: Some(&restricted),
                    io: None
                }
            )?
            .await,
        Err(Error::FilesystemAccessConfiguration(
            "local checkout publication"
        ))
    ));
    execute(tools.clone(), restricted, false).await?;
    // Legacy directory/file labels carry no whole-volume authority. Preserve
    // the task/tool grants but remove every whole-volume read/write label.
    let directory_grant = fixture
        .project
        .directory_read_capability("public", &mut None)?;
    let legacy_path_only = Capabilities::new(
        all.iter()
            .filter(|grant| {
                !grant.starts_with("volume:read:") && !grant.starts_with("volume:write:")
            })
            .chain([directory_grant.as_str()]),
    );
    assert!(matches!(
        super::super::access::require_mount_access(
            tools.workspace.view().workspace(),
            &tools.workspace.mounts,
            &tools.signed,
            &legacy_path_only,
            "local bash",
        )
        .await,
        Err(Error::FilesystemAccessConfiguration("local bash"))
    ));
    execute(tools.clone(), legacy_path_only, true).await?;
    let mut policy: Policy =
        serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY).map_err(storage)?;
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
                crate::conversation::ContentAccess::default()
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
    execute(tools, all, false).await?;
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
        &harness.verifier,
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

#[tokio::test]
async fn a_written_file_ref_is_immediately_editable() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    let (written, _) = publisher
        .stage_once(
            OperationId::new(),
            "result.txt",
            b"before edit\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    // Before the fix the returned version named the staged generation, and
    // capturing the write's own host metadata made that generation stale.
    let (edited, _) = publisher
        .stage_at(OperationId::new(), &written, b"after edit\n", None, None)?
        .await?;
    assert_eq!(edited.path(), "result.txt");
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"after edit\n"
    );
    // The edit's own reference is just as current.
    publisher
        .stage_at(OperationId::new(), &edited, b"third\n", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"third\n"
    );
    Ok(())
}

// Negative control: rebinding never hides a foreign change. A user edit made
// after the write still makes the returned reference stale.
#[tokio::test]
async fn a_foreign_change_after_a_write_still_conflicts() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    let (written, _) = publisher
        .stage_once(
            OperationId::new(),
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let path = directory.path().join("result.txt");
    std::fs::write(&path, b"user bytes\n").map_err(storage)?;
    let conflict =
        match publisher.stage_at(OperationId::new(), &written, b"overwrite\n", None, None) {
            Ok(staged) => staged.await.err(),
            Err(error) => Some(error),
        };
    let conflict = conflict.ok_or_else(|| Error::Invalid("stale reference was accepted".into()))?;
    assert_eq!(
        conflict.code(),
        crate::wire::ErrorCode::Conflict,
        "{conflict}"
    );
    assert_eq!(std::fs::read(&path).map_err(storage)?, b"user bytes\n");
    Ok(())
}

// Recovery returns the reference the write returned, and it stays editable.
// Negative control: before recovery rebound its reference, observing the
// operation returned the staged generation, and the edit conflicted.
#[tokio::test]
async fn a_recovered_write_returns_the_written_reference() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let (written, _) = publisher
        .stage_once(
            operation,
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let (observed, _) = publisher
        .observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let observed = observed.ok_or_else(|| Error::Invalid("the write was not recovered".into()))?;
    assert_eq!(observed.version(), written.version());
    publisher
        .stage_at(OperationId::new(), &observed, b"edited\n", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"edited\n"
    );
    Ok(())
}

/// Whether an edit through the reference that a replayed write returns
/// succeeds, after the first attempt stopped between its physical write and
/// that write's capture. The replay either reruns the write or, as the write
/// tool does on restart, recovers the operation first.
async fn replay_after_an_interrupted_capture_is_editable(
    skip: bool,
    recover: bool,
) -> Result<bool> {
    let (directory, _harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let write = || {
        publisher.stage_once(
            operation,
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )
    };
    KILL_BEFORE_CAPTURE.set(true);
    let interrupted = match write() {
        Ok(staged) => staged.await.err(),
        Err(error) => Some(error),
    };
    KILL_BEFORE_CAPTURE.set(false);
    assert!(
        interrupted.is_some(),
        "the first attempt was not interrupted"
    );
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"agent bytes\n"
    );
    REPLAY_SKIPS_CAPTURE.set(skip);
    let replayed = if recover {
        match publisher.observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        ) {
            Ok(observed) => observed.await.map(|(file, _)| file),
            Err(error) => Err(error),
        }
    } else {
        match write() {
            Ok(staged) => staged.await.map(|(file, _)| Some(file)),
            Err(error) => Err(error),
        }
    };
    REPLAY_SKIPS_CAPTURE.set(false);
    let replayed = replayed?
        .ok_or_else(|| Error::Invalid("the interrupted write was not recovered".into()))?;
    let edited = match publisher.stage_at(OperationId::new(), &replayed, b"edited\n", None, None) {
        Ok(staged) => staged.await.map(|_| ()),
        Err(error) => Err(error),
    };
    Ok(edited.is_ok())
}

#[tokio::test]
async fn a_replay_finishes_an_interrupted_capture() -> Result<()> {
    assert!(replay_after_an_interrupted_capture_is_editable(false, false).await?);
    // Negative control: a replay that skips the pending capture returns the
    // staged reference, which the write's own host metadata makes stale.
    assert!(!replay_after_an_interrupted_capture_is_editable(true, false).await?);
    Ok(())
}

#[tokio::test]
async fn recovery_finishes_an_interrupted_capture() -> Result<()> {
    assert!(replay_after_an_interrupted_capture_is_editable(false, true).await?);
    // Negative control: recovery that skips the pending capture returns the
    // staged reference, which the write's own host metadata makes stale.
    assert!(!replay_after_an_interrupted_capture_is_editable(true, true).await?);
    Ok(())
}

/// The reference that names `file`'s staged generation: the parent of the
/// capture that `file` names.
async fn staged_reference(harness: &Fixture, file: &FileRef) -> Result<FileRef> {
    let workspace = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view()
        .workspace()
        .clone();
    let digest: [u8; 32] = hex::decode(file.version())
        .map_err(storage)?
        .try_into()
        .map_err(|_| Error::Invalid("file version is not a generation".into()))?;
    let stop = acyclic_fs::CancellationToken::new();
    let parents = workspace
        .generation(
            acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(digest)),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .map_err(storage)?
        .value
        .parents(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
        .await
        .map_err(storage)?
        .value;
    let [staged] = parents.as_slice() else {
        return Err(Error::Invalid("the capture has no unique parent".into()));
    };
    FileRef::new(
        file.volume().clone(),
        file.path().to_owned(),
        hex::encode(staged.digest().into_bytes()),
        file.descriptor().clone(),
        file.display_name().to_owned(),
        &mut None,
    )
}

// Recovery finds the write's capture by its operation, however many commits
// have buried it, so a retried edit through the recovered reference replays.
// Negative control: the staged reference, which recovery returned once the
// capture lay deeper than a bounded walk from the head, makes that retry fail.
#[tokio::test]
async fn a_recovered_write_keeps_its_reference_however_far_the_head_moves() -> Result<()> {
    let (directory, harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let (written, _) = publisher
        .stage_once(
            operation,
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let edit = OperationId::new();
    publisher
        .stage_at(edit, &written, b"edited\n", None, None)?
        .await?;
    // Bury the capture well past any walk bounded at 64 with commits that
    // leave the head's content, and so the checkout, unchanged.
    let workspace = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view()
        .workspace()
        .clone();
    for _ in 0..33 {
        workspace
            .write_text("/buried", "x")
            .await
            .map_err(storage)?;
        workspace.remove("/buried").await.map_err(storage)?;
    }
    let (recovered, _) = publisher
        .observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let recovered =
        recovered.ok_or_else(|| Error::Invalid("the write was not recovered".into()))?;
    assert_eq!(recovered.version(), written.version());
    publisher
        .stage_at(edit, &recovered, b"edited\n", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"edited\n"
    );
    let staged = staged_reference(&harness, &written).await?;
    assert_ne!(staged.version(), written.version());
    let retried = match publisher.stage_at(edit, &staged, b"edited\n", None, None) {
        Ok(retry) => retry.await.map(|_| ()),
        Err(error) => Err(error),
    };
    assert!(matches!(&retried, Err(Error::Conflict(_))), "{retried:?}");
    Ok(())
}

// Another write between an interrupted capture and its recovery cannot take
// the interrupted write's capture: the next capture first finishes the
// capture the interrupted write durably owes, under that write's own
// operation. Recovery then returns that capture, the reference an
// uninterrupted write returns. Negative control: when the other write's
// capture records the interrupted write as its own, no capture exists under
// the write's operation and recovery returns the staged reference.
#[tokio::test]
async fn another_write_before_recovery_leaves_the_interrupted_capture_its_own() -> Result<()> {
    let (directory, harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    KILL_BEFORE_CAPTURE.set(true);
    let interrupted = match publisher.stage_once(
        operation,
        "result.txt",
        b"agent bytes
",
        "text/plain",
        "result.txt",
        crate::conversation::ContentAccess::default(),
    ) {
        Ok(staged) => staged.await.err(),
        Err(error) => Some(error),
    };
    KILL_BEFORE_CAPTURE.set(false);
    assert!(
        interrupted.is_some(),
        "the first attempt was not interrupted"
    );
    publisher
        .stage_once(
            OperationId::new(),
            "other.txt",
            b"other bytes
",
            "text/plain",
            "other.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let (recovered, _) = publisher
        .observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let recovered = recovered
        .ok_or_else(|| Error::Invalid("the interrupted write was not recovered".into()))?;
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        super::super::capture::capture_operation(write_capture(operation, "result.txt"), 1)
            .into_bytes(),
    );
    let capture = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view()
        .workspace()
        .operation_generation(
            key,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(storage)?
        .value
        .ok_or_else(|| Error::Invalid("another write took the interrupted capture".into()))?;
    assert_eq!(
        recovered.version(),
        hex::encode(capture.id().digest().into_bytes())
    );
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"agent bytes
"
    );
    assert_eq!(
        std::fs::read(directory.path().join("other.txt")).map_err(storage)?,
        b"other bytes
"
    );
    Ok(())
}

#[tokio::test]
async fn a_clean_capture_records_its_settlement_without_native_mutations() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    publisher
        .stage(
            OperationId::new(),
            "same.txt",
            b"same bytes",
            "text/plain",
            "same.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let tools = &publisher.tools;
    let view = tools.workspace.view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let staged = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    let operation = publication_operation(OperationId::new(), "same.txt");
    let owed = (
        operation,
        acyclic_fs::GitGenerationRef {
            workspace_id: view.workspace().id(),
            generation: staged,
        },
    );
    serialized(&tools.state, async |mut state| {
        tools
            .owe_write_capture((&*tools.workspace, view), &mut state.watcher, owed)
            .await?;
        tools
            .finish_owed_write(
                (&*tools.workspace, view),
                &mut state.watcher,
                PublicationPermit::Unrestricted,
            )?
            .await
    })
    .await?;
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        capture::capture_operation(OperationId::from_bytes(operation.into_bytes()), 1).into_bytes(),
    );
    let settled = view
        .workspace()
        .operation_generation(key, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    assert!(
        settled.is_some(),
        "a capture with no changes must still durably settle its debt"
    );
    let before = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    std::fs::write(directory.path().join("same.txt"), b"later user edit").map_err(storage)?;
    serialized(&tools.state, async |mut state| {
        tools
            .owe_write_capture((&*tools.workspace, view), &mut state.watcher, owed)
            .await?;
        tools
            .finish_owed_write(
                (&*tools.workspace, view),
                &mut state.watcher,
                PublicationPermit::Unrestricted,
            )?
            .await
    })
    .await?;
    assert_eq!(
        view.workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        before
    );
    assert_eq!(
        std::fs::read(directory.path().join("same.txt")).map_err(storage)?,
        b"later user edit"
    );
    Ok(())
}

#[tokio::test]
async fn a_clean_write_capture_cannot_capture_a_later_user_edit() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    let source = publisher
        .stage(
            OperationId::new(),
            "same.txt",
            b"same bytes",
            "text/plain",
            "same.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .0;
    let operation = OperationId::new();
    let captured = publisher
        .stage_at(operation, &source, b"same bytes", None, None)?
        .await?
        .0;
    let view = publisher.tools.workspace.view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        capture::capture_operation(write_capture(operation, "same.txt"), 1).into_bytes(),
    );
    let settlement = view
        .workspace()
        .operation_generation(key, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    assert!(
        settlement.is_some(),
        "even a clean capture must durably settle its operation"
    );
    let before = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    std::fs::write(directory.path().join("same.txt"), b"later user edit").map_err(storage)?;
    let replayed = publisher
        .stage_at(operation, &source, b"same bytes", None, None)?
        .await?
        .0;
    assert_eq!(replayed, captured);
    assert_eq!(
        std::fs::read(directory.path().join("same.txt")).map_err(storage)?,
        b"later user edit"
    );
    assert_eq!(
        view.workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        before
    );
    Ok(())
}
