use super::*;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};

fn write_projected(path: &Path, bytes: &[u8]) -> Result<()> {
    #[cfg(windows)]
    {
        // ProjFS suppresses notifications for its provider process. Exercise
        // the physical command boundary from a separate client process.
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new(std::env::current_exe().map_err(storage)?)
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
    let model = crate::model::Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
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
    let scratch = mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .unwrap();
    native_scope(mounts, async |claims| {
        write_projected(&scratch.destination.join("data"), b"acknowledged")?;
        Ok(((), claims))
    })
    .await?;
    assert_eq!(published(scratch, "/data").await?, b"acknowledged");

    let (started, began) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let cleaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_cleaned = Arc::clone(&cleaned);
    let destination = scratch.destination.clone();
    let mut cancelled = Box::pin(native_scope(mounts, async |claims| {
        tokio::task::spawn_blocking(move || {
            write_projected(&destination.join("data"), b"unverified").unwrap();
            started.send(()).unwrap();
            released.recv().unwrap();
            worker_cleaned.store(true, std::sync::atomic::Ordering::Release);
            claims
        })
        .await
        .map_err(storage)?;
        // No verification and no successful return: losing the capture waiter
        // leaves claims in the worker until its physical cleanup completes.
        Err::<((), Claims), _>(Error::Conflict("unverified worker".into()))
    }));
    tokio::select! {
        started = began => started.map_err(storage)?,
        result = &mut cancelled => panic!("worker did not remain active: {result:?}"),
    }
    drop(cancelled);
    let mut preflight = Box::pin(sync(mounts));
    assert!(
        futures::poll!(&mut preflight).is_pending(),
        "preflight published an active worker"
    );
    drop(preflight);
    let mut next = Box::pin(native_scope(mounts, async |claims| {
        assert!(cleaned.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            std::fs::read(scratch.destination.join("data")).map_err(storage)?,
            b"acknowledged"
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
    next.await?;
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
    let scratch = mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .unwrap();
    // Negative control: bypass command serialization exactly as the former
    // shared projection did. One verified exit also publishes the other writer.
    let root = projection_root(&scratch.destination, SCRATCH_PATH)?;
    let first = scratch.enter(root, SCRATCH_PATH, false).await?;
    let unverified = scratch.enter(root, SCRATCH_PATH, false).await?;
    write_projected(&scratch.destination.join("data"), b"unverified")?;
    scratch.leave(first).await?;
    assert_eq!(published(scratch, "/data").await?, b"unverified");
    scratch.abandon(unverified).await?;
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
#[tokio::test]
async fn task_aliases_retain_the_bound_projection_and_refuse_authored_blockers() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let task = tempfile::tempdir_in(root.path()).map_err(storage)?;
    let blocked = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let workspace = harness.workspace().unwrap();
    let original = workspace
        .mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .unwrap();
    for _ in 0..2 {
        let aliases = workspace.mounts_at(task.path()).await?;
        let alias = aliases.named(Some("scratch"))?.1;
        assert!(Arc::ptr_eq(alias.view.source.as_ref().unwrap(), original));
        assert_eq!(
            alias.native_root.as_deref(),
            Some(original.destination.as_path())
        );
        assert_eq!(
            canonical_destination(
                &std::fs::read_link(task.path().join(SCRATCH_PATH)).map_err(storage)?,
            )?,
            canonical_destination(&original.destination)?
        );
    }
    let alias = task.path().join(SCRATCH_PATH);
    std::fs::rename(&alias, task.path().join("absolute-alias")).map_err(storage)?;
    let relative = Path::new("../..").join(SCRATCH_PATH);
    link_directory(&relative, &alias).map_err(storage)?;
    let aliases = workspace.mounts_at(task.path()).await?;
    let retained = aliases.named(Some("scratch"))?.1;
    assert!(Arc::ptr_eq(
        retained.view.source.as_ref().unwrap(),
        original
    ));
    assert_eq!(
        retained.native_root.as_deref(),
        Some(original.destination.as_path())
    );
    assert_eq!(std::fs::read_link(&alias).map_err(storage)?, relative);
    // The former Windows entry point rebound the volume beneath the task
    // projection. This loses both the shared execution lock and the physical
    // root that ProjFS can actually mount.
    let mut rebound = super::super::mounts::owner(
        task.path(),
        &workspace.mounts.named(None)?.1.view.volume,
        None,
        [(
            "scratch".into(),
            workspace
                .mounts
                .named(Some("scratch"))?
                .1
                .view
                .volume
                .clone(),
            Some(Arc::clone(original)),
        )],
    )?;
    attach(
        &workspace.filesystem,
        &workspace.store,
        &acyclic_fs::hold_root(task.path()).map_err(storage)?,
        &mut rebound,
        (
            harness.issuer.as_ref().unwrap(),
            harness.signed.agent().unwrap(),
        ),
    )
    .await?;
    let rebound = rebound.named(Some("scratch"))?.1;
    assert!(!Arc::ptr_eq(
        rebound.view.source.as_ref().unwrap(),
        original
    ));
    assert_eq!(rebound.native_root, None);
    let alias = task.path().join(SCRATCH_PATH);
    std::fs::rename(&alias, task.path().join("original-alias")).map_err(storage)?;
    link_directory(blocked.path(), &alias).map_err(storage)?;
    assert!(workspace.mounts_at(task.path()).await.is_err());
    assert_eq!(
        canonical_destination(&std::fs::read_link(&alias).map_err(storage)?)?,
        canonical_destination(blocked.path())?
    );
    std::fs::create_dir(blocked.path().join(".volumes")).map_err(storage)?;
    let authored = blocked.path().join(SCRATCH_PATH);
    std::fs::write(&authored, b"authored blocker").map_err(storage)?;
    assert!(workspace.mounts_at(blocked.path()).await.is_err());
    assert_eq!(
        std::fs::read(authored).map_err(storage)?,
        b"authored blocker"
    );
    Ok(())
}

#[tokio::test]
async fn invalid_alias_is_rejected_before_projection_entry() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("scratch"))?;
    let scratch = mount.view.source.as_ref().unwrap();
    // An invalid alias must fail even when its source also has a valid name.
    // Validation precedes identity-based deduplication and lock acquisition.
    let table = super::super::mounts::owner(
        &mounts.root,
        &mounts.named(None)?.1.view.volume,
        None,
        ["scratch", "zzz"].map(|name| {
            (
                name.to_owned(),
                mount.view.volume.clone(),
                Some(Arc::clone(scratch)),
            )
        }),
    )?;
    let ran = std::sync::atomic::AtomicBool::new(false);
    let failure = native_scope(&table, async |claims| {
        ran.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(((), claims))
    })
    .await
    .expect_err("an unowned binding entered");
    assert!(
        failure.to_string().contains("outside its binding"),
        "{failure:?}"
    );
    assert!(!ran.into_inner());
    assert!(
        scratch.native().await.is_none(),
        "failed entry stranded a projection"
    );
    assert!(!scratch.destination.exists());
    // Negative control: the former exit dropped entered claims without leaving.
    // The same oracle sees that projection remain mounted.
    let root_path = projection_root(&scratch.destination, SCRATCH_PATH)?;
    drop(scratch.enter(root_path, SCRATCH_PATH, false).await?);
    assert!(
        scratch.native().await.is_some(),
        "oracle missed a stranded projection"
    );
    retire(mounts).await?;
    assert!(scratch.native().await.is_none());
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
    let (_, mount) = mounts.named(Some("scratch"))?;
    let scratch = mount.view.source.as_ref().expect("bound scratch source");
    let blocked = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(scratch.workspace.workspace().clone()),
        destination: mounts.root.join(".volumes/zzz"),
        options: scratch.options.clone(),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(crate::sync::SyncMutex::new(0)),
        execution: StateArc::new(AsyncMutex::new(())),
    });
    std::fs::write(&blocked.destination, b"authored blocker").map_err(storage)?;
    let table = super::super::mounts::owner(
        &mounts.root,
        &mounts.named(None)?.1.view.volume,
        None,
        [
            ("scratch", Arc::clone(scratch)),
            ("zzz", Arc::clone(&blocked)),
        ]
        .map(|(name, source)| (name.to_owned(), mount.view.volume.clone(), Some(source))),
    )?;
    assert_eq!(
        ordered_mounts(&table)?
            .into_iter()
            .map(|(relative, _)| relative)
            .collect::<Vec<_>>(),
        [".volumes/scratch", ".volumes/zzz"]
    );
    let ran = std::sync::atomic::AtomicBool::new(false);
    let second_entry = blocked.native().await;
    let mut scope = Box::pin(native_scope(&table, async |claims| {
        ran.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(((), claims))
    }));
    tokio::select! {
        result = &mut scope => panic!("scope returned before the first physical entry: {result:?}"),
        () = async {
            loop {
                if scratch.native().await.is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        } => {}
    }
    assert!(futures::poll!(&mut scope).is_pending());
    drop(second_entry);
    scope
        .await
        .expect_err("a regular file cannot become the second projection");
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

// Disconnect retirement attempts every volume. A volume a cancelled worker still
// owns reports its conflict, but cannot strand a later volume's projection
// until the harness drops and abandons it unpublished.
#[tokio::test]
async fn retirement_continues_past_a_volume_a_worker_still_owns() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("scratch"))?;
    let scratch = mount.view.source.as_ref().unwrap();
    // `.volumes/aaa` precedes scratch and still has one worker claim.
    let busy = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(scratch.workspace.workspace().clone()),
        destination: mounts.root.join(".volumes/aaa"),
        options: scratch.options.clone(),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(crate::sync::SyncMutex::new(1)),
        execution: StateArc::new(AsyncMutex::new(())),
    });
    let table = super::super::mounts::owner(
        &mounts.root,
        &mounts.named(None)?.1.view.volume,
        None,
        [("aaa", busy), ("scratch", Arc::clone(scratch))]
            .map(|(name, source)| (name.to_owned(), mount.view.volume.clone(), Some(source))),
    )?;
    let project = async || {
        let root = projection_root(&scratch.destination, SCRATCH_PATH)?;
        drop(scratch.enter(root, SCRATCH_PATH, false).await?);
        Ok::<_, Error>(())
    };
    project().await?;
    assert!(matches!(
        retire(&table).await,
        Err(Error::Conflict(message)) if message.contains("native workers")
    ));
    assert!(
        scratch.native().await.is_none(),
        "a conflict stranded a later projection"
    );
    assert!(!scratch.destination.exists());
    // Negative control: the former loop stopped at the first conflict. The
    // same oracle sees the later projection remain mounted.
    project().await?;
    let former = async {
        for (_, mount) in table.mounts() {
            if let Some(source) = &mount.view.source {
                Box::pin(source.retire()).await?;
            }
        }
        Ok::<_, Error>(())
    };
    assert!(former.await.is_err());
    assert!(
        scratch.native().await.is_some(),
        "oracle missed a stranded projection"
    );
    retire(mounts).await?;
    assert!(scratch.native().await.is_none());
    Ok(())
}

#[tokio::test]
async fn projection_lock_order_ignores_alias_order_and_duplicate_bindings() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let other = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("scratch"))?;
    let first = mount.view.source.as_ref().unwrap();
    let second = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(first.workspace.workspace().clone()),
        destination: other.path().join(SCRATCH_PATH),
        options: first.options.clone(),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(crate::sync::SyncMutex::new(0)),
        execution: StateArc::new(AsyncMutex::new(())),
    });
    let table = |primary: &Arc<MountedVolume>, named: &Arc<MountedVolume>| {
        super::super::mounts::owner(
            &mounts.root,
            &mount.view.volume,
            Some(Arc::clone(primary)),
            [(
                "scratch".into(),
                mount.view.volume.clone(),
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
