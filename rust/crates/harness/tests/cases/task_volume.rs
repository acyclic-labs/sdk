//! Task namespace and durable query scenario coverage.

#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness::task_volume::{
    Channel, Component, Cursor, Error, FileKey, Owner, Query, TaskVolume, Write,
};
use acyclic_stream::{MemoryStream, OwnedBytes, ReadRequest, StreamPath, StreamProvider};
use futures::StreamExt as _;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[path = "../task_volume/acceptance.rs"]
mod acceptance;
#[path = "../task_volume/heads.rs"]
mod heads;
#[path = "../task_volume/history_loss.rs"]
mod history_loss;
#[path = "../task_volume/plan_name.rs"]
mod plan_name;
#[path = "../task_volume/properties.rs"]
mod properties;
#[path = "../task_volume/recovery.rs"]
mod recovery;
#[path = "../task_volume/session.rs"]
mod session;
#[path = "../task_volume/source.rs"]
mod source;
#[path = "../task_volume/work.rs"]
mod work;

async fn append_all<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    mut generation: acyclic_harness::task_volume::Generation,
    writes: &[Write],
) -> acyclic_harness::task_volume::Result<acyclic_harness::task_volume::Generation> {
    for write in writes {
        generation = volume.append(generation, write).await?;
    }
    Ok(generation)
}

async fn memory() -> Result<(Arc<MemoryStream>, TaskVolume<MemoryStream>)> {
    let provider = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(Arc::clone(&provider), StreamPath::new("task-volume")?).await?;
    Ok((provider, volume))
}

fn write(path: &str, text: &str) -> Result<Write> {
    Ok(Write::new(FileKey::parse(path)?, text.as_bytes().to_vec()))
}

#[tokio::test]
async fn snapshot_cut_survives_publication_before_follow_registration() -> Result {
    let (provider, volume) = memory().await?;
    let other = TaskVolume::open(provider, StreamPath::new("task-volume")?).await?;
    let snapshot = volume.snapshot(&Query::Root, 10).await?;
    assert!(snapshot.files.is_empty());
    let file = write(".tasks/t/status.json", "admitted")?;
    append_all(&other, snapshot.generation, std::slice::from_ref(&file)).await?;
    let mut follow = volume.follow(Query::Root, snapshot.cursor).await?;
    let change = follow.next().await.ok_or("missing intervening commit")??;
    assert_eq!(change.file, Some(file));
    // The stream stays live; no watch event is needed to recover the next publication.
    let next = other.snapshot(&Query::Root, 10).await?;
    append_all(
        &other,
        next.generation,
        &[write(".tasks/t/status.json", "done")?],
    )
    .await?;
    assert_eq!(
        follow.next().await.ok_or("missing live change")??.file,
        Some(write(".tasks/t/status.json", "done")?)
    );
    Ok(())
}

#[tokio::test]
async fn every_accepted_cursor_reconnects_without_duplicates_or_gaps() -> Result {
    let (_, volume) = memory().await?;
    let snapshot = volume.snapshot(&Query::Root, 0).await?;
    let writes = (0..20)
        .map(|n| write(&format!(".tasks/t/notifications/n{n}.json"), "ready"))
        .collect::<Result<Vec<_>>>()?;
    append_all(&volume, snapshot.generation, &writes).await?;
    for cut in 0..=writes.len() {
        let mut cursor = snapshot.cursor.clone();
        let mut stream = volume.follow(Query::Root, cursor.clone()).await?;
        for _ in 0..cut {
            cursor = stream.next().await.ok_or("missing first prefix")??.cursor;
        }
        let encoded = serde_json::to_vec(&cursor)?;
        let recovered: Cursor = serde_json::from_slice(&encoded)?;
        drop(stream);
        let mut stream = volume.follow(Query::Root, recovered).await?;
        for expected in writes.iter().skip(cut) {
            assert_eq!(
                stream
                    .next()
                    .await
                    .ok_or("missing resumed suffix")??
                    .file
                    .as_ref(),
                Some(expected)
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.next())
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn cursor_scope_and_bounds_return_replacement_snapshots() -> Result {
    let (_, volume) = memory().await?;
    let initial = volume.snapshot(&Query::Root, 1).await?;
    append_all(&volume, initial.generation, &[write("GOAL.md", "active")?]).await?;
    let current = volume.snapshot(&Query::Root, 1).await?;
    let mut invalid = Vec::new();
    let cursor = current.cursor.clone();
    let mut c = cursor.clone();
    c.volume[0] ^= 1;
    invalid.push(c);
    let mut c = cursor.clone();
    c.epoch = uuid::Uuid::new_v4();
    invalid.push(c);
    let mut c = cursor.clone();
    c.schema += 1;
    invalid.push(c);
    let mut c = cursor.clone();
    c.query[0] ^= 1;
    invalid.push(c);
    let mut c = cursor.clone();
    c.position += 1;
    invalid.push(c);
    let mut c = cursor.clone();
    c.position = 0;
    invalid.push(c);
    let mut c = cursor.clone();
    c.commit[0] ^= 1;
    invalid.push(c);
    let mut c = initial.cursor.clone();
    c.accepted = 0;
    invalid.push(c);
    for cursor in invalid {
        assert!(matches!(
            volume.follow(Query::Root, cursor.clone()).await,
            Err(Error::ResetRequired)
        ));
        let observation = volume.observe(Query::Root, Some(cursor), 1).await?;
        assert!(observation.reset);
        let replacement = observation.snapshot.ok_or("reset omitted snapshot")?;
        assert_eq!(replacement.files, current.files);
        assert_eq!(replacement.cursor, current.cursor);
    }
    let observation = volume
        .observe(Query::File(FileKey::Goal), Some(cursor), 1)
        .await?;
    assert!(observation.reset);
    let observation = volume.observe(Query::Root, Some(current.cursor), 1).await?;
    assert!(!observation.reset);
    assert!(observation.snapshot.is_none());
    Ok(())
}

#[tokio::test]
async fn independently_opened_publishers_are_generation_fenced() -> Result {
    let (provider, volume) = memory().await?;
    let other = TaskVolume::open(provider, StreamPath::new("task-volume")?).await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    let a = [write(".tasks/t/status.json", "a")?];
    let b = [write(".tasks/t/status.json", "b")?];
    let (a, b) = tokio::join!(
        append_all(&volume, initial.generation, &a),
        append_all(&other, initial.generation, &b)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(a, Err(Error::Conflict(_))) || matches!(b, Err(Error::Conflict(_))));
    assert!(matches!(
        append_all(&other, initial.generation, &[write("GOAL.md", "stale")?]).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(volume.snapshot(&Query::Root, 10).await?.files.len(), 1);
    Ok(())
}

#[tokio::test]
async fn execution_fence_and_deadline_are_checked_in_the_publication_commit() -> Result {
    use acyclic_stream::{AppendRequest, CommitCondition, StreamError};
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Clock(AtomicU64);
    impl acyclic_stream::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    let clock = Arc::new(Clock(AtomicU64::new(100)));
    let provider = Arc::new(MemoryStream::new_with_clock(
        Default::default(),
        clock.clone(),
    ));
    let fence = StreamPath::new("execution-owner")?;
    provider
        .append(AppendRequest {
            path: fence.clone(),
            records: vec![OwnedBytes::from_static(b"held")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let volume = TaskVolume::open(provider.clone(), StreamPath::new("tasks")?).await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    // Losing ownership changes only the scheduler stream, not the task-volume tail.
    provider
        .append(AppendRequest {
            path: fence.clone(),
            records: vec![OwnedBytes::from_static(b"revoked")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    let writes = [write(".tasks/t/status.json", "done")?];
    assert!(matches!(
        volume
            .append_if(
                initial.generation,
                writes.first().ok_or("missing write")?,
                vec![CommitCondition::Tail {
                    path: fence.clone(),
                    expected: 1
                }],
                Some(101),
                None,
            )
            .await,
        Err(Error::FenceConflict)
    ));
    assert!(volume.snapshot(&Query::Root, 0).await?.files.is_empty());
    let next = volume
        .append_if(
            initial.generation,
            writes.first().ok_or("missing write")?,
            vec![CommitCondition::Tail {
                path: fence.clone(),
                expected: 2,
            }],
            Some(101),
            None,
        )
        .await?;
    clock.0.store(102, Ordering::SeqCst);
    assert!(matches!(
        volume
            .append_if(
                next,
                &write("GOAL.md", "late")?,
                vec![CommitCondition::Tail {
                    path: fence,
                    expected: 2
                }],
                Some(101),
                None,
            )
            .await,
        Err(Error::Storage(StreamError::DeadlineElapsed))
    ));
    assert_eq!(volume.snapshot(&Query::Root, 1).await?.files, writes);
    Ok(())
}

#[tokio::test]
async fn forked_histories_cannot_reuse_parent_cursors_or_publication_generations() -> Result {
    let (provider, parent) = memory().await?;
    let initial = parent.snapshot(&Query::Root, 0).await?;
    provider
        .fork(acyclic_stream::ForkRequest {
            source: StreamPath::new("task-volume")?,
            destination: StreamPath::new("fork")?,
            at_tail: Some(initial.generation.position),
            idempotency_key: None,
        })
        .await?;
    let child = TaskVolume::open(provider, StreamPath::new("fork")?).await?;
    assert!(matches!(
        child.follow(Query::Root, initial.cursor).await,
        Err(Error::ResetRequired)
    ));
    assert!(matches!(
        append_all(&child, initial.generation, &[write("GOAL.md", "foreign")?]).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn request_file_writes_are_idempotent_and_immutable() -> Result {
    let (provider, volume) = memory().await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    let request = write(".tasks/t/requests/r.json", "run")?;
    let first = volume.append(initial.generation, &request).await?;
    assert_eq!(volume.append(first, &request).await?, first);
    assert!(matches!(
        volume
            .append(first, &write(".tasks/t/requests/r.json", "different")?)
            .await,
        Err(Error::ImmutableConflict(_))
    ));
    let snapshot = volume.snapshot(&Query::Tasks, 1).await?;
    assert_eq!(
        snapshot.files,
        vec![write(".tasks/t/requests/r.json", "run")?]
    );
    assert_eq!(
        snapshot.files.first().ok_or("missing request")?.key.owner(),
        Owner::Caller
    );
    let mut inbox = provider
        .read(ReadRequest {
            path: StreamPath::new("task-volume")?,
            from: 0,
            limit: 2,
        })
        .await?;
    assert!(
        Write::from_record(&inbox.next().await.ok_or("missing inbox declaration")??)?.is_none()
    );
    assert_eq!(
        Write::from_record(&inbox.next().await.ok_or("missing inbox request")??)?,
        Some(write(".tasks/t/requests/r.json", "run")?)
    );
    Ok(())
}

#[tokio::test]
async fn exact_file_queries_and_erased_providers_share_the_same_contract() -> Result {
    let provider: Arc<dyn StreamProvider> = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(provider, StreamPath::new("tasks")?).await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    let plan = write("plans/main.md", "steps")?;
    assert_eq!(plan.key.owner(), Owner::Agent);
    append_all(
        &volume,
        initial.generation,
        &[plan.clone(), write("GOAL.md", "active")?],
    )
    .await?;
    let query = Query::folder("plans/main.md")?;
    assert_eq!(volume.snapshot(&query, 1).await?.files, vec![plan]);
    assert!(
        volume
            .snapshot(&Query::folder(".tasks/t/requests")?, 0)
            .await?
            .files
            .is_empty()
    );
    assert!(
        volume
            .snapshot(&Query::folder(".tasks/t/turns")?, 0)
            .await?
            .files
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn each_event_has_its_own_append_and_shared_payload_view() -> Result {
    let (provider, volume) = memory().await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    let writes = [
        write("GOAL.md", "goal-payload")?,
        write("plans/main.md", "plan-payload")?,
    ];
    let generation = append_all(&volume, initial.generation, &writes).await?;
    assert_eq!(generation.position, initial.generation.position + 2);
    let snapshot = volume.snapshot(&Query::Root, 2).await?;
    let mut raw = provider
        .read(ReadRequest {
            path: StreamPath::new("task-volume")?,
            from: initial.generation.position,
            limit: 2,
        })
        .await?;
    let mut follow = volume.follow(Query::Root, initial.cursor).await?;
    for expected in &writes {
        let segment = raw.next().await.ok_or("missing event append")??.value;
        let file = follow
            .next()
            .await
            .ok_or("missing change")??
            .file
            .ok_or("checkpoint")?;
        let cached = snapshot
            .files
            .iter()
            .find(|f| f.key == expected.key)
            .ok_or("missing cached file")?;
        assert_eq!(file.bytes.as_ptr(), cached.bytes.as_ptr());
        let offset = (file.bytes.as_ptr() as usize)
            .checked_sub(segment.as_ptr() as usize)
            .ok_or("copied payload")?;
        assert!(offset + file.bytes.len() <= segment.len());
        assert_eq!(
            segment
                .windows(file.bytes.len())
                .filter(|w| *w == file.bytes.as_slice())
                .count(),
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn invalid_paths_channels_and_snapshot_bounds_are_rejected() -> Result {
    for path in [
        "../GOAL.md",
        "/GOAL.md",
        "GOAL.md/",
        ".tasks/../status.json",
        ".tasks/t/turns/01/chunks/0",
        ".tasks/t/turns/0/chunks/+1",
        ".volumes/agents/../x",
        ".tasks/t/requests/.json",
        ".tasks/t/turns/0/tools/c/stdout/-1",
        ".tasks/t/plan.md",
        "plans/.md",
        "plans/../main.md",
        "plans/main.MD",
    ] {
        assert!(FileKey::parse(path).is_err(), "accepted {path}");
    }
    for path in [
        "/",
        ".tasks/",
        ".tasks/../turns/0",
        ".tasks/t/turns/00/chunks",
        "plans/",
    ] {
        assert!(Query::folder(path).is_err(), "accepted {path}");
    }
    let (_, volume) = memory().await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    assert!(matches!(
        append_all(
            &volume,
            initial.generation,
            &[write(".tasks/t/turns/0/chunks/0", "missing channel")?]
        )
        .await,
        Err(Error::InvalidSegment)
    ));
    append_all(&volume, initial.generation, &[write("GOAL.md", "goal")?]).await?;
    assert!(matches!(
        volume.snapshot(&Query::Root, 0).await,
        Err(Error::LimitExceeded)
    ));
    assert_eq!(volume.snapshot(&Query::Tasks, 0).await?.files.len(), 0);
    Ok(())
}

#[tokio::test]
async fn ten_thousand_turns_keep_publication_and_current_chunk_queries_incremental() -> Result {
    let start = Instant::now();
    let (_, volume) = memory().await?;
    let mut generation = volume.snapshot(&Query::Root, 0).await?.generation;
    for turn in 0..10_000 {
        let task = Component::new("t")?;
        let mut file = write(&format!(".tasks/t/turns/{turn}/chunks/0"), "delta")?;
        file.channel = Some(Channel::Text);
        let (next, publication_work) = work::measured(volume.append(generation, &file))?;
        generation = next;
        let (snapshot, chunk_work) =
            work::measured(volume.snapshot(&Query::Chunks { task, turn }, 1))?;
        assert_eq!(snapshot.files, vec![file]);
        let (snapshot, turn_work) = work::measured(volume.snapshot(
            &Query::Turn {
                task: Component::new("t")?,
                turn,
            },
            1,
        ))?;
        assert_eq!(snapshot.files.len(), 1);
        if matches!(turn + 1, 1_000 | 10_000) {
            assert!(publication_work <= 5);
            assert!(work::query_work_is_incremental(chunk_work));
            assert!(work::query_work_is_incremental(turn_work));
            // Deliberately replace range selection with a full scan followed by
            // filtering. The result is identical, but the same work bound fails.
            let (full, scan_work) =
                work::measured(volume.snapshot(&Query::Root, (turn + 1) as usize))?;
            let selected: Vec<_> = full
                .files
                .into_iter()
                .filter(|file| file.key == snapshot.files[0].key)
                .collect();
            assert_eq!(selected, snapshot.files);
            assert!(scan_work > turn);
            assert!(!work::query_work_is_incremental(scan_work));
            println!(
                "{} turns: publication={publication_work}, chunks={chunk_work}, turn={turn_work}, full scan={scan_work}",
                turn + 1
            );
        }
    }
    assert_eq!(generation.position, 10_001);
    println!("10k turns: {:.3}s", start.elapsed().as_secs_f64());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn disk_reopen_preserves_snapshot_and_cursor_without_physical_chunk_files() -> Result {
    use acyclic_stream::{LocalStream, LocalStreamLimits};
    let root = tempfile::tempdir_in(std::env::current_dir()?)?;
    let initial;
    let committed;
    let writes = (0..100)
        .map(|n| write(&format!(".tasks/t/notifications/n{n}.json"), "data"))
        .collect::<Result<Vec<_>>>()?;
    {
        let provider =
            Arc::new(LocalStream::open(root.path(), LocalStreamLimits::default()).await?);
        let volume = TaskVolume::open(provider, StreamPath::new("tasks")?).await?;
        let snapshot = volume.snapshot(&Query::Root, 0).await?;
        initial = snapshot.cursor;
        append_all(&volume, snapshot.generation, &writes).await?;
        committed = volume.snapshot(&Query::Root, 100).await?;
    }
    let provider = Arc::new(LocalStream::open(root.path(), LocalStreamLimits::default()).await?);
    let volume = TaskVolume::open(provider, StreamPath::new("tasks")?).await?;
    let recovered = volume.snapshot(&Query::Root, 100).await?;
    assert_eq!(recovered.files, committed.files);
    assert_eq!(recovered.cursor, committed.cursor);
    let mut follow = volume.follow(Query::Root, initial).await?;
    for expected in &writes {
        assert_eq!(
            follow
                .next()
                .await
                .ok_or("missing recovered delta")??
                .file
                .as_ref(),
            Some(expected)
        );
    }
    let names = std::fs::read_dir(root.path())?.collect::<std::io::Result<Vec<_>>>()?;
    assert!(names.len() < 10, "logical files became physical files");
    assert!(!root.path().join(".tasks").exists());
    Ok(())
}
