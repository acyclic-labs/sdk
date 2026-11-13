//! Read-your-writes revisions: parking, foreign refusal and durable incarnations.
use super::*;
use futures::StreamExt as _;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn input_revisions_follow_only_own_history_membership_and_grafts_across_replay() -> Result {
    async fn put(provider: &dyn crate::StreamProvider, path: &str) -> Result<u64> {
        match provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![Bytes::from_static(b"input").into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?
        {
            crate::AppendOutcome::Committed(receipt) => Ok(receipt.revision.index),
            _ => Err("unconditional append conflicted".into()),
        }
    }
    async fn observe(provider: &dyn crate::StreamProvider, paths: &[&str]) -> Result<crate::Heads> {
        Ok(provider
            .heads(crate::HeadsRequest {
                at: None,
                paths: paths
                    .iter()
                    .map(StreamPath::new)
                    .collect::<std::result::Result<_, _>>()?,
                retained: Default::default(),
                durable_through: None,
                projection: crate::HeadProjection::Tail,
                children: None,
                limit: paths.len().try_into()?,
                at_least: Default::default(),
            })
            .await?)
    }
    fn indexes(view: &crate::Heads) -> Vec<Option<u64>> {
        view.heads
            .iter()
            .map(|head| head.as_ref().map(|head| head.changed_at))
            .collect()
    }
    for local in [false, true] {
        let directory = tempfile::tempdir()?;
        let memory = Arc::new(crate::MemoryStream::default());
        let provider: Arc<dyn crate::StreamProvider> = if local {
            Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            memory.clone()
        };
        // A refused conditional append spends neither its retry key nor a
        // publication revision; snapshot/log replay preserves the accepted cuts.
        let opening = observe(&*provider, &["stable/source"]).await?.snapshot;
        let refused_key = crate::IdempotencyKey::unique();
        assert_eq!(
            provider
                .append(AppendRequest {
                    path: StreamPath::new("stable/source")?,
                    records: vec![Bytes::from_static(b"rejected").into()],
                    if_tail: Some(1),
                    idempotency_key: refused_key.clone(),
                })
                .await?,
            crate::AppendOutcome::TailConflict { actual_tail: 0 }
        );
        assert!(provider.inspect_idempotency(refused_key).await?.is_none());
        assert_eq!(
            observe(&*provider, &["stable/source"]).await?.snapshot,
            opening
        );
        let created = put(&*provider, "stable/source").await?;
        assert_eq!(
            created, 1,
            "refused append spent the first publication revision"
        );
        let initial = observe(&*provider, &["stable", "stable/source"]).await?;
        assert_eq!(indexes(&initial), vec![Some(created); 2]);
        let membership = put(&*provider, "stable/source/kids/a/status").await?;
        let paths = [
            "stable/source",
            "stable/source/kids",
            "stable/source/kids/a/status",
        ];
        let before = observe(&*provider, &paths).await?;
        assert_eq!(indexes(&before), vec![Some(membership); 3]);
        let progress = put(&*provider, paths[2]).await?;
        let after = observe(&*provider, &paths).await?;
        assert_eq!(
            indexes(&after),
            vec![Some(membership), Some(membership), Some(progress)]
        );
        put(&*provider, "unrelated/volume/commit").await?;
        assert_eq!(observe(&*provider, &paths).await?.heads, after.heads);
        let added = put(&*provider, "stable/source/kids/b/status").await?;
        let source = observe(&*provider, &paths).await?;
        assert_eq!(
            indexes(&source),
            vec![Some(membership), Some(added), Some(progress)]
        );
        let fork = provider
            .fork(crate::ForkRequest {
                source: StreamPath::new("stable/source")?,
                destination: StreamPath::new("stable/child")?,
                at_tail: Some(0),
                exclude: Vec::new(),
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
        let children = [
            "stable/child",
            "stable/child/kids",
            "stable/child/kids/a/status",
        ];
        let inherited = observe(&*provider, &children).await?;
        assert_eq!(indexes(&inherited), vec![Some(fork.revision.index); 3]);
        assert_eq!(observe(&*provider, &paths).await?.heads, source.heads);
        let child_progress = put(&*provider, children[2]).await?;
        let child_member = put(&*provider, "stable/child/kids/c/status").await?;
        let child = observe(&*provider, &children).await?;
        assert_eq!(
            indexes(&child),
            vec![
                Some(fork.revision.index),
                Some(child_member),
                Some(child_progress)
            ]
        );
        let nested = provider
            .fork(crate::ForkRequest {
                source: StreamPath::new("stable/source")?,
                destination: StreamPath::new("stable/child/grandchild")?,
                at_tail: Some(0),
                exclude: Vec::new(),
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
        let selected = [
            "stable/child/grandchild/kids/a/status",
            "stable/child/kids",
            "missing",
        ];
        let pinned = observe(&*provider, &selected).await?;
        assert_eq!(
            indexes(&pinned),
            vec![Some(nested.revision.index), Some(child_member), None]
        );
        put(&*provider, paths[2]).await?;
        assert_eq!(observe(&*provider, &selected).await?.heads, pinned.heads);
        // The exact same derived input indexes survive both snapshot restore
        // and the Local log/reopen path; no persisted-format side table exists.
        let reopened: Arc<dyn crate::StreamProvider> = if local {
            drop(provider);
            Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            let encoded = memory.encode_state().await;
            let restored = crate::MemoryStream::default();
            restored.restore_state(&encoded).await?;
            Arc::new(restored)
        };
        let replayed = observe(&*reopened, &selected).await?;
        assert_eq!(replayed.heads, pinned.heads);
        let late = reopened
            .head(StreamPath::new(paths[2])?)
            .await?
            .last
            .ok_or("missing latest row")?;
        let historical_request = crate::HeadsRequest {
            at: Some(crate::Revision {
                index: before.snapshot.index,
                ..replayed.snapshot.clone()
            }),
            at_least: crate::RevisionCut::of(replayed.applied.clone()),
            paths: [
                StreamPath::new(paths[2])?,
                StreamPath::new("stable/source/kids/b/status")?,
            ]
            .into_iter()
            .collect(),
            retained: [crate::RetainedRecord {
                commit_id: late.commit_id,
                path: StreamPath::new(paths[2])?,
                sequence: late.sequence,
            }]
            .into(),
            children: Some(crate::ChildrenPageRequest {
                parent: Some(StreamPath::new("stable/source/kids")?),
                after: None,
                offset: 0,
                hierarchy_version: None,
                limit: 1,
                tail_stream: Some(StreamPath::new("status")?),
            }),
            limit: 4,
            projection: crate::HeadProjection::Tail,
            durable_through: None,
        };
        let historical = reopened.heads(historical_request.clone()).await?;
        historical.validate_for(&historical_request)?;
        assert_eq!(historical.snapshot.index, before.snapshot.index);
        assert_eq!(historical.applied, replayed.applied);
        assert_eq!(historical.heads[0], before.heads[2]);
        assert!(historical.heads[1].is_none());
        assert!(
            historical.retained[0].record.is_none(),
            "a future publication leaked into an old cut"
        );
        let page = historical
            .children
            .as_ref()
            .ok_or("missing old membership")?;
        assert_eq!(page.children.len(), 1);
        assert_eq!(page.children[0].path.as_str(), "stable/source/kids/a");
        assert_eq!(page.children[0].tail, Some(1));
        assert!(page.next_after.is_none());
        let mut foreign = historical_request.clone();
        foreign.at.as_mut().ok_or("cut")?.incarnation ^= 1;
        assert_eq!(
            reopened.heads(foreign).await,
            Err(StreamError::RevisionForeign)
        );
        let mut future = historical_request;
        future.at.as_mut().ok_or("cut")?.index = replayed.applied.index + 1;
        assert_eq!(reopened.heads(future).await, Err(StreamError::Unavailable));
        let wire = crate::wire_codec::heads_to_wire(replayed.clone())?;
        assert_eq!(
            crate::wire_codec::heads_from_response(wire.clone())?,
            replayed
        );
        let mut impossible = wire;
        impossible.heads[0]
            .head
            .as_mut()
            .ok_or("missing head")?
            .changed_at = replayed.snapshot.index + 1;
        assert_eq!(
            crate::wire_codec::heads_from_response(impossible),
            Err(StreamError::InvalidResponse)
        );
    }
    Ok(())
}

fn path() -> StreamPath {
    StreamPath::new("revisions").unwrap()
}

async fn append(provider: &LocalStream, key: crate::IdempotencyKey) -> Result<crate::Revision> {
    match provider
        .append(AppendRequest {
            path: path(),
            records: vec![Bytes::from_static(b"value").into()],
            if_tail: None,
            idempotency_key: key,
        })
        .await?
    {
        crate::AppendOutcome::Committed(receipt) => Ok(receipt.revision),
        crate::AppendOutcome::TailConflict { .. } => Err("unconditional append conflicted".into()),
    }
}

async fn read(provider: &LocalStream, at_least: crate::RevisionCut) -> Result<usize> {
    let records = provider
        .read(ReadRequest {
            key: None,
            path: path(),
            from: 0,
            limit: 16,
            at_least,
        })
        .await?;
    Ok(records.collect::<Vec<_>>().await.len())
}

#[tokio::test]
async fn a_lagging_read_parks_until_its_minimum_applies() -> Result {
    let directory = tempfile::tempdir()?;
    let provider =
        Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?);
    let first = append(&provider, crate::IdempotencyKey::unique()).await?;
    // Negative control: no minimum serves the local cut at once.
    assert_eq!(read(&provider, crate::RevisionCut::default()).await?, 1);
    let next = crate::Revision {
        index: first.index + 1,
        ..first.clone()
    };
    let parked = read(&provider, crate::RevisionCut::of(next));
    tokio::pin!(parked);
    assert!(
        futures::poll!(parked.as_mut()).is_pending(),
        "a read above the applied cut must park"
    );
    append(&provider, crate::IdempotencyKey::unique()).await?;
    assert_eq!(parked.await?, 2);
    Ok(())
}

#[tokio::test]
async fn a_foreign_token_is_refused_and_an_own_token_survives_reopen() -> Result {
    let outer = tempfile::tempdir()?;
    let directory = outer.path().join("store");
    // Another spelling of the same directory.
    let alias = outer.path().join("side").join("..").join("store");
    std::fs::create_dir(outer.path().join("side"))?;
    let key = crate::IdempotencyKey::unique();
    let provider = LocalStream::open(&directory, LocalStreamLimits::default()).await?;
    let issued = append(&provider, key.clone()).await?;
    // Recover more commands than the bounded replay channel can hold. Both
    // channel capacity and replay validation must yield the controlled worker
    // on their original events, rather than blocking the seeded driver.
    for _ in 0..REPLAY_PIPELINE_COMMANDS {
        append(&provider, crate::IdempotencyKey::unique()).await?;
    }
    let foreign = crate::AppliedRevisions::in_process().at(0);
    assert_eq!(
        provider
            .wait_applied(&crate::RevisionCut::of(foreign))
            .await,
        Err(StreamError::RevisionForeign)
    );
    drop(provider);

    // A reopen, even through another path spelling, keeps the scope and the
    // incarnation, and a replay carries the original revision.
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::seeded(71);
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let provider = schedule
        .scope(LocalStream::open(&alias, LocalStreamLimits::default()))
        .await?;
    schedule.drained().await;
    driver.abort();
    assert_eq!(
        schedule
            .durable_writes()
            .iter()
            .filter(|write| write.operation == "file:sync_parent")
            .count(),
        1,
        "recovered records require one durable journal namespace publication"
    );
    let served = provider
        .wait_applied(&crate::RevisionCut::of(issued.clone()))
        .await?;
    assert!(served.same_authority(&issued) && served.index >= issued.index);
    assert_eq!(append(&provider, key).await?, issued);
    assert_eq!(
        provider.tail(path()).await?,
        u64::try_from(REPLAY_PIPELINE_COMMANDS)? + 1,
        "recovery must retain every command and deduplicate the original append"
    );
    drop(provider);

    // Negative control: wiping the same store issues a new incarnation, which
    // refuses the earlier token instead of matching its reused index.
    for entry in std::fs::read_dir(&directory)? {
        std::fs::remove_file(entry?.path())?;
    }
    let provider = LocalStream::open(&directory, LocalStreamLimits::default()).await?;
    append(&provider, crate::IdempotencyKey::unique()).await?;
    assert_eq!(
        provider.wait_applied(&crate::RevisionCut::of(issued)).await,
        Err(StreamError::RevisionForeign)
    );
    Ok(())
}

#[tokio::test]
async fn a_run_that_issued_revisions_ahead_of_a_flush_rotates_its_incarnation() -> Result {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    // Negative control: a device-flushed write keeps the record in place.
    let flushed = append(&provider, crate::IdempotencyKey::unique()).await?;
    assert!(directory.path().join(INCARNATION_FILE).exists());
    // A deferred write's revision can be issued before its frame is durable;
    // a crash could lose it and recovery would reissue its index.
    let deferred =
        crate::deferring_durability(append(&provider, crate::IdempotencyKey::unique())).await?;
    assert!(!directory.path().join(INCARNATION_FILE).exists());
    drop(provider);

    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    for issued in [flushed, deferred] {
        assert_eq!(
            provider.wait_applied(&crate::RevisionCut::of(issued)).await,
            Err(StreamError::RevisionForeign)
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_old_snapshot_version_refuses_without_repairing_disk() -> Result {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let memory = MemoryStream::new(limits.memory);
    memory
        .append(AppendRequest {
            path: path(),
            records: vec![Bytes::from_static(b"value").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let state = memory.encode_state().await;
    let snapshot = directory.path().join(SNAPSHOT_FILE);
    let current = encode_snapshot(limits, 0, &state)?;
    std::fs::write(&snapshot, &current)?;
    let provider = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(read(&provider, crate::RevisionCut::default()).await?, 1);
    drop(provider);
    let mut obsolete_state = state;
    obsolete_state[b"ACYCLIC-STREAM-STATE-V".len()] = b'1';
    let obsolete = encode_snapshot(limits, 0, &obsolete_state)?;
    std::fs::write(&snapshot, &obsolete)?;
    let error = match LocalStream::open(directory.path(), limits).await {
        Err(error) => error,
        Ok(_) => return Err("old snapshot was accepted".into()),
    };
    assert!(matches!(
        error,
        LocalStreamError::UnsupportedVersion { format: "state" }
    ));
    assert_eq!(std::fs::read(&snapshot)?, obsolete);
    // The refusal releases ownership and leaves current bytes usable.
    std::fs::write(&snapshot, current)?;
    let provider = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(read(&provider, crate::RevisionCut::default()).await?, 1);
    Ok(())
}
