use super::*;

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_local_fork_releases_its_authority_and_content() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = unbounded!(fs, create_workspace("repo"))?;
        main.write_text("/base.txt", "base").await?;
        let base = unbounded!(main, head())?;
        let agent = fork_workspace(
            &main,
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
        agent
            .write_text("/agent.txt", &"only the agent ".repeat(8_192))
            .await?;
        assert_eq!(fs.authority().authorities(64).await?.len(), 3);
        let key = IdempotencyKey::new();
        assert_eq!(agent.delete(key).await?, WorkspaceDelete::Deleted);
        assert_eq!(agent.delete(key).await?, WorkspaceDelete::AlreadyDeleted);
        assert_eq!(
            fs.delete_workspace("agent", key).await?,
            WorkspaceDelete::AlreadyDeleted
        );
        assert!(unbounded!(fs, open_workspace("agent")).is_err());
        assert!(unbounded!(fs, create_workspace("agent")).is_err());
        assert_eq!(
            fs.authority().authorities(64).await?.len(),
            2,
            "the fork's authority is gone; its base is still retained"
        );
        let collected = fs.collect_local_garbage(None, &cancellation).await?;
        assert!(collected.removed >= 1, "{collected:?}");
        let again = fs.collect_local_garbage(None, &cancellation).await?;
        assert_eq!(again.removed, 0);
        assert_eq!(
            unbounded!(main, read("/base.txt", 64))?,
            OwnedBytes::from_static(b"base")
        );
        drop((main, agent));
        close_local(fs).await?;
    }

    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    assert_eq!(
        fs.authority().authorities(64).await?.len(),
        1,
        "the fork base went with its fork"
    );
    let main = unbounded!(fs, open_workspace("repo"))?;
    assert_eq!(
        unbounded!(main, read("/base.txt", 64))?,
        OwnedBytes::from_static(b"base")
    );
    let content = "only the agent ".repeat(8_192);
    main.write_text("/again.txt", &content).await?;
    drop(main);
    close_local(fs).await?;
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = unbounded!(fs, open_workspace("repo"))?;
    assert_eq!(
        unbounded!(main, read("/again.txt", 1 << 20))?,
        OwnedBytes::from(content.into_bytes())
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collections_run_alongside_publications() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    unbounded!(fs, create_workspace("repo"))?;
    let main = unbounded!(fs, open_workspace("repo"))?;
    let mut removed = 0_u64;
    let mut concurrent_collections = 0;
    for round in 0..24_u32 {
        // Readiness drives one collection alongside each publication round.
        // Repeating collections while the same writer is pending only adds I/O.
        let collection = async {
            concurrent_collections += 1;
            fs.collect_local_garbage(None, &cancellation).await
        };
        let publication = async {
            main.write_text("/churn.txt", &round.to_string().repeat(20_000))
                .await?;
            let agent = fork_workspace(
                &main,
                &format!("agent-{round}"),
                ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
            )
            .await?;
            agent
                .write_text("/agent.txt", &format!("agent {round} ").repeat(8_192))
                .await?;
            agent.delete(IdempotencyKey::new()).await?;
            Ok::<_, WorkspaceError>(())
        };
        let (collected, published) = tokio::join!(collection, publication);
        removed += collected?.removed;
        published?;
    }
    assert_eq!(
        concurrent_collections, 24,
        "one concurrent collection must cover each publication round"
    );
    drop(main);
    removed += fs.collect_local_garbage(None, &cancellation).await?.removed;
    assert!(removed > 0);
    close_local(fs).await?;

    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = unbounded!(fs, open_workspace("repo"))?;
    assert_eq!(
        unbounded!(main, read("/churn.txt", 1 << 20))?,
        OwnedBytes::from("23".repeat(20_000).into_bytes())
    );
    assert_eq!(
        fs.collect_local_garbage(None, &cancellation).await?.removed,
        0
    );
    Ok(())
}

/// Account-level collections that sweep whatever no root retains.
#[cfg(all(feature = "local", any(unix, windows)))]
async fn account_collections(
    fs: &Fs<crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
) -> Result<(), Box<dyn Error>> {
    use acyclic_objects::Blobs as _;
    // Admission covers one cut; only a root survives the next ones.
    for _ in 0..3 {
        fs.local_account().collect().await?;
    }
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publication_straddling_an_epoch_switch_stays_retained() -> Result<(), Box<dyn Error>> {
    use acyclic_objects::Blobs as _;
    // The straddling publication joins a fork: its merged root holds both
    // sides' files, so it is reachable only through the record it has not
    // appended yet, and no checkout or pin holds it. It retains under the
    // epoch the collection then releases, so the mark reaches it only
    // because the collection waits for it to become durable.
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    let (before, forked, released) = {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = unbounded!(fs, create_workspace("repo"))?;
        main.write_text("/live.txt", "live").await?;
        let agent = fork_workspace(
            &main,
            "agent",
            ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
        )
        .await?;
        agent.write_text("/agent.txt", "agent").await?;
        main.write_text("/main.txt", "main").await?;
        // Only ids leave the block: a generation handle holds the root open.
        let before = unbounded!(main, head())?.id();
        let forked = unbounded!(agent, head())?.id();
        let plan = unbounded!(agent.join_into(&main), plan())?;
        let epoch = fs.local_epoch();
        let collection = fs.local_collection();
        // A merge join retains its source parent, then its merged closure:
        // the second retain is the one that straddles.
        let pause = epoch.hooks.pause_at(2);
        let join = plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        );
        let collector = async {
            // The join retained its merged closure under epoch 0, before any
            // switch, and has not published.
            epoch.hooks.retained.notified().await;
            assert_eq!(epoch.current(), 0, "the join retained after the switch");
            let resume = async {
                // The join stays paused until the drain blocks on it, or, with
                // no drain, until epoch 0 is released: the listing, mark and
                // release then all ran before its record.
                let released = tokio::select! {
                    () = collection.draining.notified() => false,
                    () = epoch.hooks.released.notified() => true,
                };
                drop(pause);
                released
            };
            let (collected, released) =
                tokio::join!(fs.collect_local_pass(None, true, &cancellation), resume);
            (collected, released)
        };
        let (joined, (collected, released)) = tokio::join!(join, collector);
        let JoinOutcome::Applied(_) = joined? else {
            return Err("the clean join did not publish".into());
        };
        collected?;
        assert_eq!(epoch.current(), 1);
        account_collections(&fs).await?;
        drop(plan);
        drop(agent);
        drop(main);
        close_local(fs).await?;
        (before, forked, released)
    };
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = unbounded!(fs, open_workspace("repo"))?;
    let merged = unbounded!(main, head())?.id();
    assert!(
        merged != before && merged != forked,
        "the join's root is not fresh"
    );
    let fresh = acyclic_objects::Digest::from_bytes(merged.digest().into_bytes());
    let account = fs.local_account();
    let mut stored = Vec::new();
    let mut after = None;
    loop {
        let page = account.list(after, 1_024).await?;
        stored.extend(page.digests);
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert!(stored.contains(&fresh), "the merged root is not stored");
    // Only epoch 1's root is left to retain it, so the account refuses to
    // delete it.
    assert_eq!(
        account.delete(fresh).await?.outcome,
        acyclic_objects::Delete::Referenced
    );
    drop(account);
    drop(main);
    close_local(fs).await?;
    // The root survived because the drain held the collection, not because
    // the window was missed.
    assert!(
        !released,
        "epoch 0 was released while the join was paused before its record"
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_publication_never_holds_a_collection() -> Result<(), Box<dyn Error>> {
    // A publication that retained and is then dropped before its record
    // releases its gate, so the collection draining it finishes.
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
    let main = unbounded!(fs, create_workspace("repo"))?;
    main.write_text("/kept.txt", "kept").await?;
    let epoch = fs.local_epoch();
    let collection = fs.local_collection();
    let _pause = epoch.hooks.pause();
    let mut abandoned = Box::pin(main.write_text("/abandoned.txt", "never published"));
    tokio::select! {
        _ = &mut abandoned => return Err("the paused publication finished".into()),
        () = epoch.hooks.retained.notified() => {}
    }
    let collected = {
        let mut collector = std::pin::pin!(fs.collect_local_pass(None, true, &cancellation));
        tokio::select! {
            _ = &mut collector => return Err("the collection finished without draining".into()),
            () = collection.draining.notified() => {}
        }
        // Negative control: while the publication lives, the collection waits.
        for _ in 0..64 {
            tokio::task::yield_now().await;
            assert!(futures::poll!(collector.as_mut()).is_pending());
        }
        drop(abandoned);
        collector.await
    };
    let _ = collected?;
    drop(main);
    close_local(fs).await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_at_every_epoch_step_loses_nothing_and_is_reclaimed_after()
-> Result<(), Box<dyn Error>> {
    use crate::logical_objects::epoch::Step;
    use acyclic_objects::Blobs as _;
    for step in [
        Step::Persisted,
        Step::Switched,
        Step::Retained,
        Step::Released,
    ] {
        let directory = tempfile::tempdir()?;
        let root = directory.path();
        let cancellation = CancellationToken::new();
        {
            let fs = Fs::local(crate::LocalOptions::new(root)).await?;
            let main = unbounded!(fs, create_workspace("repo"))?;
            main.write_text("/live.txt", "live").await?;
            let agent = fork_workspace(
                &main,
                "agent",
                ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
            )
            .await?;
            agent
                .write_text("/dead.txt", &"only the agent ".repeat(8_192))
                .await?;
            agent.delete(IdempotencyKey::new()).await?;
            *fs.local_epoch()
                .hooks
                .crash_after
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(step);
            // Two interrupted collections in a row: the second advances past
            // the epoch the first left unreleased.
            for _ in 0..2 {
                assert!(
                    fs.collect_local_pass(None, true, &cancellation)
                        .await
                        .is_err()
                );
            }
            drop((main, agent));
            close_local(fs).await?;
        }
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        // No live object depends on the epoch the crash left half open.
        account_collections(&fs).await?;
        let main = unbounded!(fs, open_workspace("repo"))?;
        assert_eq!(
            unbounded!(main, read("/live.txt", 64))?,
            OwnedBytes::from_static(b"live"),
            "{step:?}"
        );
        // The next collection reclaims whatever the crash left retained.
        fs.collect_local_garbage(None, &cancellation).await?;
        let (_, retained) = fs.collect_local_pass(None, false, &cancellation).await?;
        assert_eq!(retained, 0, "{step:?}");
        let current = fs.local_epoch().current();
        for earlier in 0..current {
            let released = fs
                .local_account()
                .release(&crate::logical_objects::Epoch::root(earlier))
                .await?;
            assert!(!released.existed, "{step:?} left epoch {earlier}");
        }
        assert_eq!(
            unbounded!(main, read("/live.txt", 64))?,
            OwnedBytes::from_static(b"live")
        );
        drop(main);
        close_local(fs).await?;
    }
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quiet_account_collection_opens_no_epoch() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
    let main = unbounded!(fs, create_workspace("repo"))?;
    main.write_text("/live.txt", &"live ".repeat(8_192)).await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    let epoch = fs.local_epoch();
    let settled = epoch.current();
    // Nothing unreachable is retained, so nothing is retained again or
    // released: the collection only reads.
    let quiet = fs.collect_local_garbage(None, &cancellation).await?;
    assert_eq!(quiet.removed, 0);
    assert_eq!(epoch.current(), settled);
    // Negative control: opening an epoch anyway retains every live object
    // again.
    fs.collect_local_pass(None, true, &cancellation).await?;
    assert_eq!(epoch.current(), settled + 1);
    assert_eq!(
        unbounded!(main, read("/live.txt", 1 << 20))?.len(),
        5 * 8_192
    );
    drop(main);
    close_local(fs).await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publication_during_an_epoch_switch_never_waits_on_the_collector()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
    let first = unbounded!(fs, create_workspace("first"))?;
    let second = unbounded!(fs, create_workspace("second"))?;
    let epoch = fs.local_epoch();
    let collection = fs.local_collection();
    let pause = epoch.hooks.pause();
    let in_flight = first.write_text("/in-flight.txt", "admitted before the switch");
    let collector = async {
        epoch.hooks.retained.notified().await;
        fs.collect_local_pass(None, true, &cancellation).await
    };
    let started_during = async {
        // The collector is draining the publication that retained first.
        collection.draining.notified().await;
        let mut during = std::pin::pin!(second.write_text("/during.txt", "admitted during"));
        let finished_first = tokio::select! {
            published = &mut during => {
                published?;
                true
            }
            () = collection.gate_waits.notified() => false,
        };
        drop(pause);
        if !finished_first {
            during.await?;
        }
        Ok::<_, WorkspaceError>(finished_first)
    };
    let (in_flight, collected, finished_first) = tokio::join!(in_flight, collector, started_during);
    in_flight?;
    collected?;
    assert!(
        finished_first?,
        "an admission waited on the collection's drain"
    );
    assert_eq!(
        unbounded!(second, read("/during.txt", 64))?,
        OwnedBytes::from_static(b"admitted during")
    );
    drop((first, second));
    close_local(fs).await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_dropped_mid_drain_leaves_its_drain_to_the_next() -> Result<(), Box<dyn Error>>
{
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
    let main = unbounded!(fs, create_workspace("repo"))?;
    let epoch = fs.local_epoch();
    let collection = fs.local_collection();
    let pause = epoch.hooks.pause();
    let in_flight = main.write_text("/in-flight.txt", "retained under epoch 0");
    let collector = async {
        epoch.hooks.retained.notified().await;
        {
            // The first collection switches the epoch, then is dropped while
            // it waits on the paused publication.
            let first = fs.collect_local_pass(None, true, &cancellation);
            tokio::pin!(first);
            let finished = tokio::select! {
                _ = &mut first => true,
                () = collection.draining.notified() => false,
            };
            assert!(!finished, "a collection finished past a paused publication");
        }
        let watch = async {
            // The next collection drains the publication before it releases
            // epoch 0, or releases it with the publication still in flight.
            let drained = tokio::select! {
                () = collection.draining.notified() => true,
                () = epoch.hooks.released.notified() => false,
            };
            drop(pause);
            drained
        };
        tokio::join!(fs.collect_local_pass(None, true, &cancellation), watch)
    };
    let (in_flight, (collected, drained)) = tokio::join!(in_flight, collector);
    assert!(
        drained,
        "epoch 0 was released while its publication was in flight"
    );
    in_flight?;
    collected?;
    assert_eq!(
        unbounded!(main, read("/in-flight.txt", 64))?,
        OwnedBytes::from_static(b"retained under epoch 0")
    );
    drop(main);
    close_local(fs).await?;
    Ok(())
}

#[cfg(all(feature = "local", feature = "test-support", any(unix, windows)))]
#[tokio::test]
async fn a_dropped_epoch_writer_keeps_the_original_root_until_physical_completion()
-> Result<(), Box<dyn Error>> {
    use acyclic_native_runtime::{OwnershipAnchor, testing::Schedule};

    let directory = tempfile::tempdir()?;
    let fs = Fs::local_with_ownership_anchor(
        crate::LocalOptions::new(directory.path()),
        OwnershipAnchor::new(()),
    )
    .await?;
    let released = fs
        .local_root_release_barrier()
        .ok_or("missing root owner")?;
    let epoch = fs.local_epoch();
    let collection = fs.local_collection();
    let running = collection.run().await;
    let (schedule, mut events) = Schedule::new();
    let mut write = Box::pin(schedule.scope(epoch.persist(1, running, fs)));
    let submitted = tokio::select! {
        biased;
        result = &mut write => {
            drop(result?);
            return Err("epoch write ended before submission".into());
        },
        boundary = events.recv() => boundary.ok_or("epoch submission source lost")?,
    };
    assert_eq!(submitted.name, "blocking:submitted");
    drop(write);
    // Removing the physical closure's owner capture releases the actual root
    // here, even though its accepted pointer write has not started.
    assert!(
        !released.is_released(),
        "root released before accepted epoch I/O"
    );
    submitted.release();
    let completed = tokio::select! {
        biased;
        boundary = events.recv() => boundary.ok_or("epoch completion source lost")?,
        () = schedule.drained() => return Err("epoch producer ended without its completion event".into()),
    };
    assert_eq!(completed.name, "blocking:completed");
    completed.release();
    schedule.drained().await;
    acyclic_native_runtime::spawn_blocking(move || released.wait()).await?;
    // The physical result, not delivery to the cancelled caller, completes
    // the collection turn and makes the durable pointer available on reopen.
    drop(collection.run().await);
    assert_eq!(
        crate::logical_objects::Epoch::open(directory.path().join("objects-epoch"))?.current(),
        1,
    );
    let reopened = Fs::local(crate::LocalOptions::new(directory.path())).await?;
    close_local(reopened).await?;
    Ok(())
}

/// Names the root a child process holds open in `one_process_owns_a_local_root`.
#[cfg(all(feature = "local", any(unix, windows)))]
const ROOT_OWNER: &str = "ACYCLIC_FS_ROOT_OWNER";

/// Child of `one_process_owns_a_local_root`: publishes into the root it names,
/// then holds it open until killed. Outside that test it does nothing.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_root_owner_holds_its_root_until_killed() -> Result<(), Box<dyn Error>> {
    let Some(root) = std::env::var_os(ROOT_OWNER) else {
        return Ok(());
    };
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = unbounded!(fs, create_workspace("repo"))?;
    main.write_text("/owned.txt", "owned").await?;
    println!("owned");
    std::future::pending::<()>().await;
    Ok(())
}

/// Epoch switching is safe only because one process owns a local root: a
/// second process cannot open either of its stores, and a dead owner's root
/// reopens and repairs.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_process_owns_a_local_root() -> Result<(), Box<dyn Error>> {
    use std::io::BufRead as _;
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("root");
    let mut owner = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "workspace::tests::publication::collection::a_root_owner_holds_its_root_until_killed",
            "--nocapture",
        ])
        .env(ROOT_OWNER, &root)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let stdout = owner.stdout.take().ok_or("the owner has no stdout")?;
    let owned = tokio::task::spawn_blocking(move || {
        std::io::BufReader::new(stdout)
            .lines()
            .any(|line| line.is_ok_and(|line| line == "owned"))
    })
    .await?;
    assert!(owned, "the owner exited before it published");
    assert!(matches!(
        Fs::local(crate::LocalOptions::new(&root)).await,
        Err(
            crate::FsError::LocalStream(acyclic_stream::LocalStreamError::AlreadyOpen)
                | crate::FsError::LocalObjects(
                    acyclic_objects::local::LocalOpenError::AlreadyOwned
                )
        )
    ));
    assert!(matches!(
        acyclic_stream::LocalStream::open(
            root.join("stream"),
            acyclic_stream::LocalStreamLimits::default()
        )
        .await,
        Err(acyclic_stream::LocalStreamError::AlreadyOpen)
    ));
    assert!(matches!(
        acyclic_objects::LocalObjects::open(root.join("objects"), Default::default(), None).await,
        Err(acyclic_objects::local::LocalOpenError::AlreadyOwned)
    ));
    // The OS releases both locks when the owner dies; its unclean close
    // leaves the root marked for repair.
    owner.kill()?;
    owner.wait()?;
    assert!(root.join("objects").join("OPEN").exists());
    let fs = Fs::local(crate::LocalOptions::new(&root)).await?;
    let main = unbounded!(fs, open_workspace("repo"))?;
    assert_eq!(
        unbounded!(main, read("/owned.txt", 64))?,
        OwnedBytes::from_static(b"owned")
    );
    drop(main);
    close_local(fs).await?;
    Ok(())
}
