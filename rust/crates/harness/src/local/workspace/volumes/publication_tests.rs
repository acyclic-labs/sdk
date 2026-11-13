use super::*;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};

fn write_projected(path: &Path, bytes: &[u8]) -> Result<()> {
    #[cfg(windows)]
    {
        // ProjFS suppresses notifications for its provider process. Exercise
        // the physical command boundary from a separate client process.
        let output = acyclic_native_runtime::process_output(
            acyclic_native_runtime::ProcessCommand::new(std::env::current_exe().map_err(storage)?)
                .args([
                    "--exact",
                    "local::workspace::volumes::publication_tests::external_projection_writer",
                    "--nocapture",
                ])
                .env("ACYCLIC_TEST_PROJECTION_PATH", path)
                .env(
                    "ACYCLIC_TEST_PROJECTION_BYTES",
                    std::str::from_utf8(bytes).map_err(storage)?,
                ),
            std::time::Duration::from_secs(5),
            4096,
        )
        .map_err(storage)?;
        if !output.status.success() {
            return Err(Error::Storage(format!(
                "projection client failed: {output:?}"
            )));
        }
        Ok(())
    }
    #[cfg(not(windows))]
    std::fs::write(path, bytes).map_err(storage)
}

#[cfg(windows)]
#[test]
fn external_projection_writer() -> Result<()> {
    let Some(path) = crate::test_support::environment("ACYCLIC_TEST_PROJECTION_PATH") else {
        return Ok(());
    };
    let bytes = crate::test_support::environment("ACYCLIC_TEST_PROJECTION_BYTES")
        .ok_or_else(|| Error::Invalid("projection client has no bytes".into()))?;
    std::fs::write(path, bytes.to_string_lossy().as_bytes()).map_err(storage)
}

async fn open(root: &Path, state: &Path) -> Result<crate::LocalHarness> {
    let model = crate::model::Model::new("scripted".into(), "test".into(), "1".into())?;
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            },
        ),
    );
    crate::LocalHarness::open(
        crate::test_support::local_harness_config(root, (model, provider))?
            .state_directory(state)
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await
}

async fn published(scratch: &MountedVolume, path: &str) -> Result<Vec<u8>> {
    let cancellation = CancellationToken::new();
    let head = scratch
        .workspace
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    Ok(head
        .read(path, 64, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .as_ref()
        .to_vec())
}

#[tokio::test]
async fn cancelled_command_retires_before_next_entrant_and_preserves_acknowledged_head()
-> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let scratch = mounts.named(Some("home"))?.1.view.source.as_ref().unwrap();
    native_scope(mounts, async |claims| {
        // Root checkout bindings carry the same explicit native roots as task
        // aliases, and expose cwd/discovery only while execution projects them.
        let prepared = crate::filesystem::native_process::shell::prepare(
            crate::tools::ShellInput {
                command: crate::tools::ShellCommand::Source("echo root".into()),
                cwd: Some(HOME_PATH.into()),
                timeout_ms: None,
                background: false,
                target: None,
            },
            mounts,
            crate::filesystem::native_process::Invocation {
                executable: std::env::current_exe().map_err(storage)?,
                argv: Vec::new(),
                cwd: mounts.root.clone(),
                environment: Default::default(),
                inherit_environment: false,
                execution_grant: None,
                schedule: acyclic_native_runtime::CaptureSchedule::Unbounded,
                maximum_output_bytes: None,
                standard_input: None,
                process_custody: None,
            },
            0,
            std::time::Instant::now(),
        )?;
        let native = std::fs::canonicalize(&scratch.destination).map_err(storage)?;
        assert_eq!(prepared.cwd, native);
        let discovery: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&prepared.environment["ACYCLIC_WORKSPACES"]).map_err(storage)?;
        assert_eq!(
            discovery.get("home"),
            Some(&scratch.destination.to_string_lossy().into_owned())
        );
        write_projected(&scratch.destination.join("data"), b"acknowledged")?;
        Ok(((), claims))
    })
    .await?;
    assert_eq!(published(scratch, "/data").await?, b"acknowledged");

    let (started, began) = tokio::sync::oneshot::channel();
    let (retired, retirement) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let cleaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_cleaned = Arc::clone(&cleaned);
    let destination = scratch.destination.clone();
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::seeded(71);
    // The existing native admission was issued on the real clock before this
    // scope; keep that clock origin while controlling cleanup submission.
    schedule.set_unix_millis(acyclic_stream::UnixMillisClock::now_unix_millis(
        &acyclic_stream::SystemUnixMillisClock,
    ));
    let mut cancelled = Box::pin(schedule.scope(native_scope(mounts, async |claims| {
        tokio::task::spawn_blocking(move || {
            write_projected(&destination.join("data"), b"unverified").unwrap();
            started.send(()).unwrap();
            released.recv().unwrap();
            worker_cleaned.store(true, std::sync::atomic::Ordering::Release);
            drop(claims);
            retired.send(()).unwrap();
        })
        .await
        .map_err(storage)?;
        // No verification and no successful return: losing the capture waiter
        // leaves claims in the worker until its physical cleanup completes.
        Err::<((), Claims), _>(Error::Conflict("unverified worker".into()))
    })));
    let mut began = std::pin::pin!(began);
    loop {
        tokio::select! {
            started = &mut began => {
                started.map_err(storage)?;
                break;
            },
            result = &mut cancelled => panic!("worker did not remain active: {result:?}"),
            boundary = boundaries.recv() => boundary.unwrap().release(),
        }
    }
    drop(cancelled);
    let before_cleanup = schedule.trace().len();
    let mut preflight = Box::pin(sync(mounts));
    assert!(
        futures::poll!(&mut preflight).is_pending(),
        "preflight published an active worker"
    );
    drop(preflight);
    let mut next = Box::pin(native_scope(mounts, async |claims| {
        assert!(
            cleaned.load(std::sync::atomic::Ordering::Acquire),
            "next entrant outran physical cleanup"
        );
        assert_eq!(
            std::fs::read(scratch.destination.join("data")).map_err(storage)?,
            b"acknowledged",
            "cancelled command exposed unverified physical bytes"
        );
        assert_eq!(published(scratch, "/data").await?, b"acknowledged");
        write_projected(&scratch.destination.join("data"), b"next verified")?;
        Ok(((), claims))
    }));
    assert!(
        futures::poll!(&mut next).is_pending(),
        "next command entered before physical cleanup"
    );
    release.send(()).map_err(storage)?;
    // The same physical worker admits retirement before this event. Drive its
    // accepted cleanup concurrently with the command waiting for its guard.
    retirement.await.map_err(storage)?;
    let mut completed = Box::pin(async {
        let (result, ()) = futures::join!(next, schedule.drained());
        result
    });
    loop {
        tokio::select! {
            result = &mut completed => {
                result?;
                break;
            },
            boundary = boundaries.recv() => boundary.unwrap().release(),
        }
    }
    assert!(
        schedule.trace()[before_cleanup..]
            .iter()
            .any(|event| event == "task:dispatch"),
        "physical cleanup escaped its original task scope"
    );
    assert_eq!(published(scratch, "/data").await?, b"next verified");
    assert!(scratch.native().await.is_none());
    assert!(!scratch.destination.exists());
    Ok(())
}

#[tokio::test]
async fn shared_projection_publication_cannot_isolate_an_unverified_writer() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let scratch = mounts.named(Some("home"))?.1.view.source.as_ref().unwrap();
    // Negative control: bypass command serialization exactly as the former
    // shared projection did. One verified exit also publishes the other writer.
    let first = scratch.enter(false).await?;
    let unverified = scratch.enter(false).await?;
    write_projected(&scratch.destination.join("data"), b"unverified")?;
    scratch.leave(first).await?;
    assert_eq!(published(scratch, "/data").await?, b"unverified");
    scratch.abandon(unverified).await?;
    Ok(())
}

#[tokio::test]
async fn task_mounts_share_external_bindings_without_authoring_aliases() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let task = tempfile::tempdir().map_err(storage)?;
    std::fs::create_dir(task.path().join(".volumes")).map_err(storage)?;
    let authored = task.path().join(".volumes/home");
    std::fs::write(&authored, b"user content").map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let workspace = harness.workspace().unwrap();
    let original = workspace
        .mounts
        .named(Some("home"))?
        .1
        .view
        .source
        .as_ref()
        .unwrap();
    let mounts = workspace.mounts_at(task.path()).await?;
    let bound = mounts.named(Some("home"))?.1;
    assert!(Arc::ptr_eq(bound.view.source.as_ref().unwrap(), original));
    assert_eq!(
        bound.native_root.override_path().map(PathBuf::as_path),
        Some(original.destination.as_path())
    );
    assert!(original.destination.starts_with(state.path()));
    assert!(!root.path().join(".volumes").exists());
    assert_eq!(std::fs::read(authored).map_err(storage)?, b"user content");
    Ok(())
}

// Both bindings are valid. Only physical entry of the second projection fails,
// after the first has entered, so this exercises partial-entry retirement.
#[tokio::test]
async fn failed_scope_entry_retires_the_projections_it_entered() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().expect("fixture workspace").mounts;
    let (_, mount) = mounts.named(Some("home"))?;
    let scratch = mount.view.source.as_ref().expect("bound scratch source");
    let blocked = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(scratch.workspace.workspace().clone()),
        binding_root: Arc::clone(&scratch.binding_root),
        destination: scratch.binding_root.path().join("zzz"),
        options: scratch.options.clone(),
        task_view_admission: Arc::clone(&scratch.task_view_admission),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(projection::Users::default()),
        execution: StateArc::new(AsyncMutex::new(())),
        _binding: None,
    });
    std::fs::write(&blocked.destination, b"authored blocker").map_err(storage)?;
    let table = super::super::mounts::owner(
        &mounts.root,
        (
            "project",
            &mounts.named(None)?.1.view.volume,
            "",
            crate::agent::WorkspaceMode::ReadWrite,
            None,
        ),
        [("home", Arc::clone(scratch)), ("zzz", Arc::clone(&blocked))].map(|(name, source)| {
            (
                name.to_owned(),
                mount.view.volume.clone(),
                String::new(),
                crate::agent::WorkspaceMode::ReadWrite,
                Some(source),
            )
        }),
    )?;
    assert_eq!(
        ordered_mounts(&table)?
            .into_iter()
            .map(|(relative, _)| relative)
            .collect::<Vec<_>>(),
        ["volume:///home", "volume:///zzz"]
    );
    let ran = std::sync::atomic::AtomicBool::new(false);
    let error = native_scope(&table, async |claims| {
        ran.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(((), claims))
    })
    .await
    .expect_err("a regular file cannot become the second projection");
    // The second binding's refusal proves that ordered entry passed the first
    // binding. An earlier mount failure must not make retirement pass vacuously.
    assert!(
        matches!(error, Error::Conflict(message) if message == "volume alias replaces a project file"),
        "scope did not reach the authored blocker"
    );
    assert!(!ran.into_inner());
    assert!(
        scratch.native().await.is_none(),
        "failed later entry stranded the earlier projection"
    );
    assert!(!scratch.destination.exists());
    assert!(blocked.native().await.is_none());
    assert_eq!(
        std::fs::read(&blocked.destination).map_err(storage)?,
        b"authored blocker"
    );
    Ok(())
}

// Disconnect retirement waits for every worker's physical cleanup. A volume a
// worker still owns cannot prevent an idle later volume from retiring first.
#[tokio::test]
async fn retirement_continues_past_a_volume_a_worker_still_owns() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("home"))?;
    let scratch = mount.view.source.as_ref().unwrap();
    // `.volumes/aaa` precedes scratch and still has one worker claim.
    let busy = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(scratch.workspace.workspace().clone()),
        binding_root: Arc::clone(&scratch.binding_root),
        destination: scratch.binding_root.path().join("aaa"),
        options: scratch.options.clone(),
        task_view_admission: Arc::clone(&scratch.task_view_admission),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(projection::Users::default()),
        execution: StateArc::new(AsyncMutex::new(())),
        _binding: None,
    });
    let table = super::super::mounts::owner(
        &mounts.root,
        (
            "project",
            &mounts.named(None)?.1.view.volume,
            "",
            crate::agent::WorkspaceMode::ReadWrite,
            None,
        ),
        [("aaa", Arc::clone(&busy)), ("home", Arc::clone(scratch))].map(|(name, source)| {
            (
                name.to_owned(),
                mount.view.volume.clone(),
                String::new(),
                crate::agent::WorkspaceMode::ReadWrite,
                Some(source),
            )
        }),
    )?;
    let busy_claim = busy.enter(false).await?;
    drop(scratch.enter(false).await?);
    let mut retirement = Box::pin(retire(&table));
    assert!(
        futures::poll!(&mut retirement).is_pending(),
        "retirement completed while a worker still owned its projection"
    );
    let release_worker = async {
        // Retirement has started each projection. This lock waits for the
        // idle projection's physical cleanup, while the busy claim stays held.
        assert!(
            scratch.native().await.is_none(),
            "a busy worker stranded a later projection"
        );
        assert!(!scratch.destination.exists());
        busy.abandon(busy_claim).await
    };
    let (retired, released) = futures::join!(retirement, release_worker);
    released?;
    retired?;
    assert!(busy.native().await.is_none());
    assert!(!busy.destination.exists());
    Ok(())
}

#[tokio::test]
async fn projection_lock_order_ignores_alias_order_and_duplicate_bindings() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let other = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("home"))?;
    let first = mount.view.source.as_ref().unwrap();
    let second = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(first.workspace.workspace().clone()),
        binding_root: Arc::new(acyclic_fs::hold_root(other.path()).map_err(storage)?),
        destination: other.path().join("home"),
        options: first.options.clone(),
        task_view_admission: Arc::clone(&first.task_view_admission),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(projection::Users::default()),
        execution: StateArc::new(AsyncMutex::new(())),
        _binding: None,
    });
    let table = |primary: &Arc<MountedVolume>, named: &Arc<MountedVolume>| {
        super::super::mounts::owner(
            &mounts.root,
            (
                "project",
                &mount.view.volume,
                "",
                crate::agent::WorkspaceMode::ReadWrite,
                Some(Arc::clone(primary)),
            ),
            [(
                "home".into(),
                mount.view.volume.clone(),
                String::new(),
                crate::agent::WorkspaceMode::ReadWrite,
                Some(Arc::clone(named)),
            )],
        )
    };
    let forward = table(first, &second)?;
    let swapped = table(&second, first)?;
    let duplicate = table(first, first)?;
    let identities = |table: &Mounts| -> Result<Vec<PathBuf>> {
        Ok(ordered_mounts(table)?
            .into_iter()
            .map(|(_, mount)| mount.view.source.as_ref().unwrap().destination.clone())
            .collect())
    };
    assert_eq!(identities(&forward)?, identities(&swapped)?);
    assert_eq!(identities(&duplicate)?.len(), 1);
    let former = |table: &Mounts| {
        table
            .mounts()
            .filter_map(|(_, mount)| {
                mount
                    .view
                    .source
                    .as_ref()
                    .map(|source| source.destination.clone())
            })
            .collect::<Vec<_>>()
    };
    assert_ne!(
        former(&forward),
        former(&swapped),
        "alias-order control did not invert the locks"
    );
    assert_eq!(
        former(&duplicate).len(),
        2,
        "duplicate control did not select its own lock twice"
    );
    std::fs::create_dir(&first.destination).map_err(storage)?;
    native_scope(&duplicate, async |claims| Ok(((), claims))).await?;
    assert!(first.native().await.is_none());
    Ok(())
}
