use super::*;
use acyclic_harness::task_volume::{Change, Generation};
use std::sync::atomic::Ordering;
mod fixture {
    use acyclic_stream;
    include!("../../../stream/tests/support/session.rs");
}
use fixture::{Counts, Fixture, Options, Traffic};
impl Fixture {
    fn client(&self) -> Result<acyclic_stream::grpc::Client> {
        Ok(self
            .tls_client
            .as_ref()
            .ok_or("TLS fixture required")?
            .with_credential("fixture")?)
    }
}

#[tokio::test]
async fn publication_observes_share_one_streams_session_and_resume_at_the_snapshot_cut() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let volume_path = StreamPath::new("volumes/task")?;
    let writer = TaskVolume::open(Arc::clone(&provider), volume_path.clone()).await?;
    let fixture = Fixture::with_options(
        Arc::clone(&provider),
        "node",
        Options {
            tls: true,
            ..Default::default()
        },
    )
    .await?;
    let counts = &fixture.counts;
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let client = Arc::new(fixture.client()?);
        let reader = TaskVolume::open(Arc::clone(&client), volume_path.clone()).await?;
        let query = Query::Root;
        let seed = writer.snapshot(&query, 1).await?;
        let mut generation = writer.append(seed.generation, &write("GOAL.md", "initial")?).await?;
        let mut observations = futures::future::try_join_all((0..32).map(|_| reader.observe(query.clone(), None, 1))).await?;
        let mut folded = BTreeMap::new();
        for observation in &observations {
            assert!(!observation.reset);
            let snapshot = observation.snapshot.as_ref().ok_or("initial query omitted snapshot")?;
            assert_eq!(snapshot.cursor.position, generation.position);
            assert_eq!(snapshot.files.len(), 1);
            folded.insert(snapshot.files[0].key.clone(), snapshot.files[0].clone());
        }
        // Keep an unread channel saturated while independent file queries advance.
        let unread_from = generation.position;
        let mut unread = client.follow(volume_path, unread_from).await?;
        let unread_id = counts.guards.lock().map_err(|_| "guards poisoned")?.iter()
            .find(|(_, _, guard)| guard.is_empty()).ok_or("unread channel omitted")?.0;
        let mut children = client.children(acyclic_stream::ChildrenRequest { parent: Some(StreamPath::new("volumes")?), limit: 1 }).await?;
        assert_eq!(children.next().await.ok_or("discovery ended")??.path.as_str(), "volumes/task");
        let mut measured = Vec::new();
        let mut last = seed.cursor;
        for index in 0..40 {
            if index == 20 {
                assert_eq!(counts.sessions.load(Ordering::Relaxed), 1);
                assert_eq!(verify_sharing(counts, 1), Ok(()));
                fixture.reset()?;
            }
            let content = format!("goal-{index}");
            let started = Instant::now();
            generation = writer.append(generation, &write("GOAL.md", &content)?).await?;
            let mut changes = Vec::with_capacity(observations.len());
            for (view, observation) in observations.iter_mut().enumerate() {
                let change = observation.changes.next().await.ok_or("publication follow ended")??;
                if view == 0 && index >= 8 { measured.push(started.elapsed().as_nanos()); }
                changes.push(change);
            }
            assert_eq!(verify_publication(&changes, 32, generation, content.as_bytes()), Ok(()));
            let first = changes.remove(0);
            let file = first.file.ok_or("root query filtered a publication")?;
            folded.insert(file.key.clone(), file);
            last = first.cursor;
            if index == 1 {
                fixture.until(|counts| flow(counts, 1, unread_id, 1).is_some_and(|flow|
                    flow.positions.len() == 2 && flow.granted_frames == 2)).await;
                let saturated = flow(counts, 1, unread_id, 1).ok_or("missing unread flow")?;
                assert_eq!(saturated.positions, [unread_from + 1, unread_from + 2]);
                assert_eq!(saturated.granted_frames - saturated.positions.len() as u64, 0);
                assert!(saturated.sent_bytes > 0 && saturated.sent_bytes <= saturated.granted_bytes);
            }

        }
        let saved_fold = folded.values().cloned().collect::<Vec<_>>();
        drop(observations.remove(0));
        generation = writer.append(generation, &write("GOAL.md", "between-tiers")?).await?;
        assert!(generation.position > last.position);
        let pinned = reader.snapshot_at(&query, last.clone(), 1).await?;
        assert_eq!(pinned.cursor, last);
        assert_eq!(pinned.files, saved_fold);
        assert_ne!(reader.snapshot(&query, 1).await?.files, saved_fold);
        let mut resumed = reader.observe(query.clone(), Some(last.clone()), 1).await?;
        assert!(!resumed.reset);
        assert!(resumed.snapshot.is_none());
        let change = resumed.changes.next().await.ok_or("resume ended")??;
        assert_eq!(change.cursor.position, last.position + 1);
        assert_eq!(change.cursor.commit, generation.commit);
        let file = change.file.ok_or("resumed change omitted file")?;
        folded.insert(file.key.clone(), file);
        let mut incompatible = change.cursor;
        incompatible.commit[0] ^= 1;
        let reset = reader.observe(query.clone(), Some(incompatible), 1).await?;
        assert!(reset.reset);
        let snapshot = reset.snapshot.ok_or("reset omitted replacement")?;
        assert_eq!(snapshot.files, folded.values().cloned().collect::<Vec<_>>());
        assert_eq!(writer.snapshot(&query, 1).await?.files, snapshot.files);
        let _later = TaskVolume::open(Arc::clone(&client), StreamPath::new("volumes/later")?).await?;
        assert_eq!(children.next().await.ok_or("discovery ended")??.path.as_str(), "volumes/later");
        assert_eq!(verify_sharing(counts, 2), Ok(()));
        assert_eq!(counts.follows.load(Ordering::Relaxed) - counts.children.load(Ordering::Relaxed), 68);
        assert_eq!(counts.children.load(Ordering::Relaxed), 2);
        assert_eq!(counts.actors.load(Ordering::Relaxed), 0);
        // Drain the formerly blocked channel through every publication, including
        // the reset. Old buffered generations must not duplicate or lose a cut.
        for (offset, sequence) in (unread_from..generation.position).enumerate() {
            let record = unread.next().await.ok_or("unread follow ended")??;
            assert_eq!(record.sequence, sequence);
            let file = Write::from_record(&record)?.ok_or("publication omitted file")?;
            let expected = if offset < 40 { format!("goal-{offset}") } else { "between-tiers".into() };
            assert_eq!(file.bytes.as_ref(), expected.as_bytes());
        }
        assert!(futures::poll!(unread.next()).is_pending());
        let drained = flow(counts, 2, unread_id, 2).ok_or("resumed unread flow omitted")?;
        assert_eq!(drained.positions, (unread_from + 1..=generation.position).collect::<Vec<_>>());
        assert!(drained.granted_frames >= drained.positions.len() as u64);
        measured.sort_unstable();
        println!("publication_session follows=68 children=2 sessions=2 connections=1 actor_channels=0 samples=32 p50_ns={} p99_ns={}", measured[15], measured[31]);
        drop(unread);
        drop(children);
        drop(observations);
        drop(resumed);
        drop(reset.changes);
        // Every Session must close before force-closing the fixture server.
        fixture.until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 2).await;
        Ok(()) as Result
    }).await;
    result?
}

fn flow(counts: &Counts, session: usize, channel: u64, generation: u64) -> Option<Traffic> {
    counts
        .traffic
        .lock()
        .expect("fixture traffic poisoned")
        .get(&(session, channel, generation))
        .cloned()
}
fn verify_sharing(counts: &Counts, sessions: usize) -> std::result::Result<(), &'static str> {
    if counts.sessions.load(Ordering::Relaxed) != sessions {
        return Err("session sharing");
    }
    if counts.connections.load(Ordering::Relaxed) != 1 {
        return Err("connection sharing");
    }
    if counts.actors.load(Ordering::Relaxed) != 0 {
        return Err("actor channel opened");
    }
    Ok(())
}
fn verify_publication(
    changes: &[Change],
    expected: usize,
    generation: Generation,
    content: &[u8],
) -> std::result::Result<(), &'static str> {
    if changes.len() != expected {
        return Err("missing publication delivery");
    }
    for change in changes {
        if change.cursor.position != generation.position
            || change.cursor.commit != generation.commit
        {
            return Err("publication cursor");
        }
        if change
            .file
            .as_ref()
            .is_none_or(|file| file.key != FileKey::Goal || file.bytes.as_ref() != content)
        {
            return Err("publication content");
        }
    }
    Ok(())
}

#[tokio::test]
async fn unshared_volume_queries_fail_the_same_sharing_verifier_without_a_timeout() -> Result {
    let memory = Arc::new(MemoryStream::default());
    let writer = TaskVolume::open(Arc::clone(&memory), StreamPath::new("unshared")?).await?;
    let fixture = Fixture::with_options(
        memory,
        "node",
        Options {
            tls: true,
            ..Default::default()
        },
    )
    .await?;
    let mut observations = Vec::new();
    for _ in 0..3 {
        let reader =
            TaskVolume::open(Arc::new(fixture.client()?), StreamPath::new("unshared")?).await?;
        observations.push(reader.observe(Query::Root, None, 1).await?);
    }
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 3);
    assert_eq!(verify_sharing(&fixture.counts, 1), Err("session sharing"));
    drop(observations);
    drop(writer);
    fixture
        .until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 3)
        .await;
    Ok(())
}

#[tokio::test]
async fn a_missed_publication_fails_the_same_cursor_verifier_without_a_timeout() -> Result {
    let memory = Arc::new(MemoryStream::default());
    let path = StreamPath::new("missed")?;
    let writer = TaskVolume::open(Arc::clone(&memory), path.clone()).await?;
    let fixture = Fixture::with_options(
        memory,
        "node",
        Options {
            tls: true,
            ..Default::default()
        },
    )
    .await?;
    let reader = TaskVolume::open(Arc::new(fixture.client()?), path).await?;
    let observation = reader.observe(Query::Root, None, 1).await?;
    let initial = observation.snapshot.ok_or("snapshot omitted")?.generation;
    // Deliberately broken delivery configuration suppresses the first publication.
    let mut broken = observation.changes.skip(1);
    let first = writer.append(initial, &write("GOAL.md", "first")?).await?;
    writer.append(first, &write("GOAL.md", "second")?).await?;
    let delivered = broken.next().await.ok_or("broken follow ended")??;
    assert_eq!(
        verify_publication(&[delivered], 1, first, b"first"),
        Err("publication cursor")
    );
    drop(broken);
    fixture
        .until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 1)
        .await;
    Ok(())
}
