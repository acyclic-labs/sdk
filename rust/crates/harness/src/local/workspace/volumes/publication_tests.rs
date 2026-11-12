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
        .read(path, 4096, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .as_ref()
        .to_vec())
}

// This command's own view of the home volume.
fn home(claims: &Claims) -> Result<PathBuf> {
    claims
        .mounts()
        .named(Some("home"))?
        .1
        .native_root
        .override_path()
        .cloned()
        .ok_or_else(|| Error::NotFound("home view".into()))
}

// (i) and (iv): a verified command publishes only after its verification; a
// command whose caller is lost discards its whole view, and the acknowledged
// head survives it.
#[tokio::test]
async fn lost_caller_discards_its_whole_view_and_keeps_the_acknowledged_head() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let scratch = mounts.named(Some("home"))?.1.view.source.as_ref().unwrap();
    native_scope(mounts, async |claims| {
        let view = home(&claims)?;
        // The command's cwd and discovery name its own view, never the
        // shared binding.
        let prepared = crate::filesystem::native_process::shell::prepare(
            crate::tools::ShellInput {
                command: crate::tools::ShellCommand::Source("echo root".into()),
                cwd: Some(HOME_PATH.into()),
                timeout_ms: None,
                background: false,
                target: None,
            },
            claims.mounts(),
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
        )?;
        assert_ne!(view, scratch.destination);
        assert_eq!(prepared.cwd, std::fs::canonicalize(&view).map_err(storage)?);
        assert_eq!(
            prepared.environment.get("ACYCLIC_VOLUME_HOME"),
            Some(&view.to_string_lossy().into_owned())
        );
        write_projected(&view.join("data"), b"acknowledged")?;
        assert!(
            published(scratch, "/data").await.is_err(),
            "an unverified view was published"
        );
        Ok(((), claims))
    })
    .await?;
    assert_eq!(published(scratch, "/data").await?, b"acknowledged");

    let (started, began) = tokio::sync::oneshot::channel();
    let (retired, retirement) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::seeded(71);
    // The existing native admission was issued on the real clock before this
    // scope; keep that clock origin while controlling the discard's task.
    schedule.set_unix_millis(acyclic_stream::UnixMillisClock::now_unix_millis(
        &acyclic_stream::SystemUnixMillisClock,
    ));
    let mut lost = Box::pin(schedule.scope(native_scope(mounts, async |claims| {
        let view = home(&claims)?;
        tokio::task::spawn_blocking(move || {
            write_projected(&view.join("data"), b"unverified").unwrap();
            write_projected(&view.join("partial"), b"unverified").unwrap();
            started.send(()).unwrap();
            released.recv().unwrap();
            drop(claims);
            retired.send(()).unwrap();
        })
        .await
        .map_err(storage)?;
        // No verification and no successful return: the worker owns the
        // claims until its physical cleanup completes.
        Err::<((), Claims), _>(Error::Conflict("unverified worker".into()))
    })));
    let mut began = std::pin::pin!(began);
    loop {
        tokio::select! {
            started = &mut began => {
                started.map_err(storage)?;
                break;
            },
            result = &mut lost => panic!("worker did not remain active: {result:?}"),
            boundary = boundaries.recv() => boundary.unwrap().release(),
        }
    }
    drop(lost);
    assert_eq!(published(scratch, "/data").await?, b"acknowledged");
    release.send(()).map_err(storage)?;
    retirement.await.map_err(storage)?;
    let mut drained = std::pin::pin!(schedule.drained());
    loop {
        tokio::select! {
            () = &mut drained => break,
            boundary = boundaries.recv() => boundary.unwrap().release(),
        }
    }
    // All or nothing: neither unverified write reached the volume.
    assert_eq!(published(scratch, "/data").await?, b"acknowledged");
    assert!(published(scratch, "/partial").await.is_err());
    native_scope(mounts, async |claims| {
        let view = home(&claims)?;
        assert_eq!(
            std::fs::read(view.join("data")).map_err(storage)?,
            b"acknowledged"
        );
        assert!(!view.join("partial").exists());
        Ok(((), claims))
    })
    .await?;
    Ok(())
}

// Concurrent commands on one volume never serialize at admission. While the
// first command is inside its scope, holding its own view, a second command
// enters, writes and hands back; only then does the first leave. The negative
// control admits each command behind one exclusive per-volume guard, the
// shape this replaced: the second command finds it held.
#[tokio::test]
async fn concurrent_commands_never_serialize_at_admission() -> Result<()> {
    for serialized in [false, true] {
        let root = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let harness = open(root.path(), state.path()).await?;
        let mounts = &harness.workspace().unwrap().mounts;
        let scratch = mounts.named(Some("home"))?.1.view.source.as_ref().unwrap();
        let gate = tokio::sync::Mutex::new(());
        let admit = || {
            if !serialized {
                return Ok(None);
            }
            gate.try_lock()
                .map(Some)
                .map_err(|_| Error::Conflict("admission is serialized".into()))
        };
        let (entered, first_inside) = tokio::sync::oneshot::channel();
        let (finished, second_finished) = tokio::sync::oneshot::channel::<Result<()>>();
        let first = async {
            let _admitted = admit()?;
            native_scope(mounts, async move |claims| {
                entered
                    .send(())
                    .map_err(|_| Error::Conflict("second command dropped".into()))?;
                // Stay inside this scope until the second command's whole
                // lifetime, hand-back included, has ended.
                let second = second_finished.await.map_err(storage)?;
                Ok((second, claims))
            })
            .await
        };
        let second = async {
            first_inside.await.map_err(storage)?;
            let result = async {
                let _admitted = admit()?;
                native_scope(mounts, async |claims| {
                    write_projected(&home(&claims)?.join("second"), b"second")?;
                    Ok(((), claims))
                })
                .await
            }
            .await;
            finished
                .send(result)
                .map_err(|_| Error::Conflict("first command dropped".into()))
        };
        let (first, second) = futures::join!(first, second);
        second?;
        let second_inside_first = first?;
        if serialized {
            assert!(
                matches!(&second_inside_first, Err(Error::Conflict(message)) if message.contains("serialized")),
                "the negative control did not serialize: {second_inside_first:?}"
            );
        } else {
            second_inside_first?;
            assert_eq!(published(scratch, "/second").await?, b"second");
        }
    }
    Ok(())
}

// (ii): a trivial command, preflight included, completes while another
// command's caller is lost and its worker has not yet cleaned up.
#[tokio::test]
async fn a_trivial_command_never_waits_for_a_lost_caller() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (started, began) = tokio::sync::oneshot::channel();
    let (retired, retirement) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let mut lost = Box::pin(native_scope(mounts, async |claims| {
        tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            released.recv().unwrap();
            drop(claims);
            retired.send(()).unwrap();
        })
        .await
        .map_err(storage)?;
        Err::<((), Claims), _>(Error::Conflict("unverified worker".into()))
    }));
    tokio::select! {
        started = began => started.map_err(storage)?,
        result = &mut lost => panic!("worker did not remain active: {result:?}"),
    }
    drop(lost);
    sync(mounts).await?;
    native_scope(mounts, async |claims| Ok(((), claims))).await?;
    release.send(()).map_err(storage)?;
    retirement.await.map_err(storage)?;
    Ok(())
}

// (iii): concurrent commands' disjoint writes both land. Their overlapping
// write lands with git conflict markers holding both sides, and the later
// command's result names the path; neither side is silently overwritten.
#[tokio::test]
async fn disjoint_writes_land_and_overlapping_writes_carry_git_markers() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let scratch = mounts.named(Some("home"))?.1.view.source.as_ref().unwrap();
    native_scope(mounts, async |claims| {
        write_projected(&home(&claims)?.join("shared"), b"base\n")?;
        Ok(((), claims))
    })
    .await?;
    let (forked, b_forked) = tokio::sync::oneshot::channel();
    let (landed, a_landed) = tokio::sync::oneshot::channel();
    let a = async {
        let result = native_scope(mounts, async move |claims| {
            let view = home(&claims)?;
            write_projected(&view.join("a"), b"a")?;
            write_projected(&view.join("shared"), b"a\n")?;
            b_forked.await.map_err(storage)?;
            Ok(((), claims))
        })
        .await;
        landed.send(()).unwrap();
        result
    };
    let b = native_scope(mounts, async move |claims| {
        let view = home(&claims)?;
        forked.send(()).unwrap();
        write_projected(&view.join("b"), b"b")?;
        write_projected(&view.join("shared"), b"b\n")?;
        a_landed.await.map_err(storage)?;
        assert!(
            !view.join("a").exists(),
            "a view saw another command's write"
        );
        Ok(((), claims))
    });
    let (a, b) = futures::join!(a, b);
    a?;
    let error = b.expect_err("the overlapping hand-back must report its conflict");
    assert!(
        matches!(&error, Error::Conflict(message) if message.contains("volume:///home/shared")),
        "{error:?}"
    );
    assert_eq!(published(scratch, "/a").await?, b"a");
    assert_eq!(published(scratch, "/b").await?, b"b");
    let shared = String::from_utf8(published(scratch, "/shared").await?).map_err(storage)?;
    assert!(
        shared.contains("<<<<<<<")
            && shared.contains("\na\n")
            && shared.contains("\nb\n")
            && shared.contains(">>>>>>>"),
        "{shared}"
    );
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

// Disconnect retirement waits for every host session to leave. A volume a
// session still uses cannot prevent an idle later volume from retiring first.
#[tokio::test]
async fn retirement_continues_past_a_volume_a_session_still_uses() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = &harness.workspace().unwrap().mounts;
    let (_, mount) = mounts.named(Some("home"))?;
    let scratch = mount.view.source.as_ref().unwrap();
    // `.volumes/aaa` precedes scratch and still has one host session.
    let busy = Arc::new(MountedVolume {
        workspace: crate::tools::git::OwnedView::Volume(scratch.workspace.workspace().clone()),
        binding_root: Arc::clone(&scratch.binding_root),
        destination: scratch.binding_root.path().join("aaa"),
        options: scratch.options.clone(),
        task_view_admission: Arc::clone(&scratch.task_view_admission),
        native: StateArc::new(AsyncMutex::new(None)),
        users: StateArc::new(projection::Users::default()),
        authority: None,
        _binding: None,
    });
    let table = super::super::mounts::owner(
        &mounts.root,
        &mounts.named(None)?.1.view.volume,
        None,
        [("aaa", Arc::clone(&busy)), ("home", Arc::clone(scratch))]
            .map(|(name, source)| (name.to_owned(), mount.view.volume.clone(), Some(source))),
    )?;
    let busy_claim = busy
        .enter_with_options(busy.options.clone().unwrap())
        .await?;
    drop(
        scratch
            .enter_with_options(scratch.options.clone().unwrap())
            .await?,
    );
    let mut retirement = Box::pin(retire(&table));
    assert!(
        futures::poll!(&mut retirement).is_pending(),
        "retirement completed while a session still used its projection"
    );
    let release_worker = async {
        // Retirement has started each projection. This lock waits for the
        // idle projection's physical cleanup, while the busy session stays.
        assert!(
            scratch.native().await.is_none(),
            "a busy session stranded a later projection"
        );
        assert!(!scratch.destination.exists());
        drop(busy_claim);
        Ok::<_, Error>(())
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
        authority: None,
        _binding: None,
    });
    let table = |primary: &Arc<MountedVolume>, named: &Arc<MountedVolume>| {
        super::super::mounts::owner(
            &mounts.root,
            &mount.view.volume,
            Some(Arc::clone(primary)),
            [(
                "home".into(),
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
    // One view per volume, however many names bind it: every alias names it.
    native_scope(&duplicate, async |claims| {
        let roots: std::collections::BTreeSet<_> = claims
            .mounts()
            .mounts()
            .filter_map(|(_, mount)| mount.native_root.override_path().cloned())
            .collect();
        assert_eq!(roots.len(), 1);
        assert!(!roots.contains(&first.destination));
        Ok(((), claims))
    })
    .await?;
    assert!(first.native().await.is_none());
    Ok(())
}
