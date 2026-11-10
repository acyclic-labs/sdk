//! Original file calls through the hosted actor's committed reply settlement.
//! Private model payloads need an ordering barrier before their publication;
//! their deferred journal frames share the dispatch phase's device drain.
//! History adds neither. One independent durable commit adds exactly one drain.
use super::*;
use acyclic_harness::{model::ModelEvent, test_support::ScriptedEvent};

fn flushes(writes: &[DurableWrite]) -> (usize, usize) {
    writes.iter().fold((0, 0), |(barriers, drains), write| {
        if write.operation.ends_with("_barrier") {
            (barriers + 1, drains)
        } else {
            let drain = matches!(
                write.operation,
                "file:sync"
                    | "file:sync_data"
                    | "file:sync_all"
                    | "file:sync_parent"
                    | "file:sync_filesystem"
                    | "file:sync_checkpoint_batch"
            );
            (barriers, drains + usize::from(drain))
        }
    })
}

#[derive(Debug)]
struct Windows {
    open: (usize, usize),
    first: (usize, usize),
    replies: [(usize, usize); 5],
    reopen: (usize, usize),
    reopened_first: (usize, usize),
}

/// Opens, settles five replies, commits one independent record, then reopens
/// and settles one more reply. Each turn cut follows committed settlement.
fn turns(
    schedule: acyclic_native_runtime::testing::Schedule,
    directory: &Path,
    replies: [ScriptedTurn; 6],
) -> EntryFuture<'_, Result<[usize; 10]>> {
    entry(async move {
        let reference =
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
        let volume = VolumeRef::new(
            reference.clone(),
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("dst".into()),
            &mut None,
        )?;
        let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
            .await
            .map_err(storage)?;
        FilesystemHost::new(fs, &reference, None)?
            .create_volume(&volume)
            .await?;
        let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
        let expected: Vec<String> = replies
            .iter()
            .map(|reply| match reply {
                ScriptedTurn::Text(text) => text.clone(),
                ScriptedTurn::Events(_) => "streamed reply".into(),
            })
            .collect();
        let provider = Arc::new(ScriptedModelProvider::new(replies));
        let config = || -> Result<_> {
            Ok(local_harness_config(
                directory.join("filesystem"),
                (model.clone(), provider.clone()),
            )?
            .without_user_settings()
            .volume(volume.clone())
            .state_directory(directory.join("state")))
        };
        let mut cuts = [0; 10];
        cuts[0] = schedule.durable_write_count();
        let harness = Arc::new(entry(LocalHarness::open(config()?)).await?);
        cuts[1] = schedule.durable_write_count();
        let (agents, _) = entry(harness.agents()).await?;
        for (turn, expected) in expected[..5].iter().enumerate() {
            prompt(&harness, &format!("turn-{turn}"), "reply", expected).await?;
            // Join the serving dispatcher's delivery and committed checkpoint.
            entry(agents.runtime().complete()).await.map_err(storage)?;
            cuts[turn + 2] = schedule.durable_write_count();
        }
        use acyclic_stream::StreamProviderExt as _;
        agents
            .provider()
            .append(acyclic_stream::AppendRequest {
                path: acyclic_stream::StreamPath::new("dst/extra").map_err(storage)?,
                records: vec!["extra".into()],
                if_tail: None,
                idempotency_key: acyclic_stream::IdempotencyKey::unique(),
            })
            .await
            .map_err(storage)?;
        cuts[7] = schedule.durable_write_count();
        drop(agents);
        drop(harness);
        let reopened = Arc::new(entry(LocalHarness::open(config()?)).await?);
        cuts[8] = schedule.durable_write_count();
        prompt(&reopened, "reopened", "reply", &expected[5]).await?;
        let (agents, _) = entry(reopened.agents()).await?;
        entry(agents.runtime().complete()).await.map_err(storage)?;
        cuts[9] = schedule.durable_write_count();
        Ok(cuts)
    })
}

/// Observe open, settled replies, the independent commit and reopen. Teardown
/// drains every accepted worker outside those windows.
fn measure(replies: [ScriptedTurn; 6]) -> Result<Windows> {
    let runtime = acyclic_native_runtime::testing::seeded_runtime(71).map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let directory = tempfile::tempdir().map_err(storage)?;
        let mut driver = driver::Driver::new(71, None);
        let driver::Outcome::Completed(cuts) = driver
            .run(turns(driver.schedule.clone(), directory.path(), replies))
            .await
        else {
            panic!("uncut reply publication crashed");
        };
        let cuts = cuts?;
        let writes = driver.schedule.durable_writes();
        let window = |from: usize, to: usize| flushes(&writes[cuts[from]..cuts[to]]);
        if window(0, 1) != (7, 12) {
            eprintln!(
                "fresh open original calls: {:#?}",
                &writes[cuts[0]..cuts[1]]
            );
            let sources: std::collections::BTreeSet<_> =
                driver.boundaries.iter().map(|(_, detail)| detail).collect();
            eprintln!("original operation owners: {sources:#?}");
        }
        Ok(Windows {
            open: window(0, 1),
            first: window(1, 2),
            replies: std::array::from_fn(|index| window(index + 2, index + 3)),
            reopen: window(7, 8),
            reopened_first: window(8, 9),
        })
    })
}

fn text(reply: &str) -> ScriptedTurn {
    ScriptedTurn::Text(reply.into())
}

#[test]
fn reply_publication_drains_follow_commits_and_not_history_or_event_count() -> Result<()> {
    // Equal text in 2 or 32 content deltas; only the event count differs.
    let streamed = |parts: usize| {
        let mut events: Vec<ScriptedEvent> = (0..parts)
            .map(|part| {
                let delta = match part {
                    0 => "streamed",
                    _ if part + 1 == parts => " reply",
                    _ => "",
                };
                ScriptedEvent::Event(ModelEvent::Content {
                    delta: delta.into(),
                })
            })
            .collect();
        events.push(ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::json!({"finish": "stop"}),
        }));
        ScriptedTurn::Events(events)
    };
    let observed = measure([
        text("one"),
        text("two"),
        text("three"),
        streamed(2),
        streamed(32),
        text("four"),
    ])?;
    assert_eq!(
        observed.open,
        (7, 12),
        "fresh open flushes changed: {observed:?}"
    );
    assert_eq!(observed.reopen, (1, 5), "reopen flushes changed");
    // Root creation publishes its policy and namespaces before the first reply.
    // WAL commits may coalesce, but the settled reply windows remain exact.
    assert_eq!(
        observed.first.0, 12,
        "first-turn barriers changed: {observed:?}"
    );
    assert!(
        observed.first.1 <= 18,
        "first-turn drains changed: {observed:?}"
    );
    assert_eq!(
        observed.reopened_first,
        (9, 12),
        "reopened placement changed: {observed:?}"
    );
    // Includes input publication through committed actor settlement. This
    // absolute budget also rejects uniform added or missing reply drains.
    // Thirty extra content parts need thirty extra ordering barriers.
    assert_eq!(
        observed.replies,
        [(8, 11), (8, 11), (9, 11), (39, 11), (0, 1)],
        "settled reply publication budget changed"
    );
    Ok(())
}

#[test]
fn negative_control_native_drains_are_counted() -> Result<()> {
    use acyclic_fs::{LocalCoreStateStore, WorkspaceLineageRecord, WorkspaceLineageStore};

    let directory = tempfile::tempdir().map_err(storage)?;
    let runtime = acyclic_native_runtime::testing::seeded_runtime(71).map_err(storage)?;
    let workspace_id = acyclic_fs::WorkspaceId::from_bytes([1; 16]);
    let generation = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([2; 32]));
    let record = WorkspaceLineageRecord {
        version: 1,
        revision: 1,
        ready: true,
        workspace_id,
        workspace_name: "checkpoint".into(),
        parent_workspace_id: None,
        parent_workspace_name: None,
        fork_generation: generation,
        initial_generation: generation,
    };
    let mut driver = driver::Driver::new(71, None);
    let store = LocalCoreStateStore::new(directory.path());
    runtime.block_on(async {
        let driver::Outcome::Completed(result) = driver
            .run(entry(async {
                // Ordinary transactions checkpoint after their WAL commit is flushed.
                assert!(
                    WorkspaceLineageStore::compare_and_swap(
                        &store,
                        workspace_id,
                        0,
                        record.clone()
                    )
                    .await
                    .map_err(storage)?
                );
                Ok::<_, Error>(())
            }))
            .await
        else {
            panic!("uncut checkpoint crashed");
        };
        result
    })?;
    assert_eq!(
        runtime
            .block_on(WorkspaceLineageStore::load(&store, workspace_id))
            .map_err(storage)?,
        Some(record)
    );
    let writes = driver.schedule.durable_writes();
    let operation = if cfg!(target_os = "macos") {
        "file:sync_checkpoint_batch"
    } else {
        "file:sync_all"
    };
    let checkpoint = writes
        .iter()
        .find(|write| write.operation == operation)
        .expect("the committed ordinary transaction must checkpoint its record");
    assert_eq!(
        flushes(std::slice::from_ref(checkpoint)),
        (0, 1),
        "the oracle missed the native checkpoint drain"
    );
    #[cfg(target_os = "linux")]
    {
        std::fs::write(directory.path().join("dirty"), b"durable").map_err(storage)?;
        let start = driver.schedule.durable_write_count();
        runtime.block_on(async {
            let driver::Outcome::Completed(result) = driver
                .run(entry(async {
                    acyclic_native_runtime::sync_filesystem(directory.path())
                }))
                .await
            else {
                panic!("uncut filesystem sync crashed");
            };
            result.map_err(storage)
        })?;
        assert_eq!(
            flushes(&driver.schedule.durable_writes()[start..]),
            (0, 1),
            "the oracle missed the native filesystem-wide drain"
        );
    }
    Ok(())
}
