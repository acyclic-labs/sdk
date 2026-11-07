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

#[derive(Debug, Eq, PartialEq)]
struct Windows {
    replies: [(usize, usize); 5],
    reopened_reply: (usize, usize),
}

/// Establishes custody and placement before measuring settled replies, then
/// repeats that placement setup after reopen. Cuts follow committed settlement.
fn turns(
    schedule: acyclic_native_runtime::testing::Schedule,
    directory: &Path,
    replies: [ScriptedTurn; 7],
) -> EntryFuture<'_, Result<[usize; 8]>> {
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
        let mut cuts = [0; 8];
        let harness = Arc::new(entry(LocalHarness::open(config()?)).await?);
        let (agents, root) = entry(harness.agents()).await?;
        // Opening can publish private signing custody. Its durable file and
        // namespace installation is setup, rather than reply publication.
        // The first actual input also claims the actor's durable epoch before
        // external handler work, fencing any previous runtime after a crash.
        prompt(&harness, "initial-placement", "reply", &expected[0]).await?;
        entry(agents.drain(root)).await.map_err(storage)?;
        cuts[0] = schedule.durable_write_count();
        for (turn, expected) in expected[1..5].iter().enumerate() {
            prompt(&harness, &format!("turn-{turn}"), "reply", expected).await?;
            // The reply is observable before its serving delivery retires.
            // Wait on that same actor's delivery lock and committed checkpoint,
            // while its ordinary continuous runtime remains running.
            entry(agents.drain(root)).await.map_err(storage)?;
            cuts[turn + 1] = schedule.durable_write_count();
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
        cuts[5] = schedule.durable_write_count();
        drop(harness);
        let reopened = Arc::new(entry(LocalHarness::open(config()?)).await?);
        let (agents, root) = entry(reopened.agents()).await?;
        // An idle drain cannot claim an epoch: only an actual delivery does.
        // Settle that required placement fence before measuring the next reply.
        prompt(&reopened, "reopened-placement", "reply", &expected[5]).await?;
        entry(agents.drain(root)).await.map_err(storage)?;
        cuts[6] = schedule.durable_write_count();
        prompt(&reopened, "reopened-reply", "reply", &expected[6]).await?;
        entry(agents.drain(root)).await.map_err(storage)?;
        cuts[7] = schedule.durable_write_count();
        Ok(cuts)
    })
}

/// Observe settled replies and an independent commit after custody/placement
/// setup. Teardown drains accepted work outside those windows.
fn measure(replies: [ScriptedTurn; 7]) -> Result<()> {
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
        let observed = Windows {
            replies: std::array::from_fn(|index| window(index, index + 1)),
            reopened_reply: window(6, 7),
        };
        assert_eq!(
            observed,
            Windows {
                // Equal streamed text in 2 or 32 parts adds ordering barriers,
                // while the independent final commit adds one device drain.
                replies: [(8, 11), (8, 11), (9, 11), (39, 11), (0, 1)],
                reopened_reply: (8, 11),
            },
            "window durability operations: {:?}; recent committed work: {:?}",
            std::array::from_fn::<_, 7, _>(|index| {
                writes[cuts[index]..cuts[index + 1]]
                    .iter()
                    .filter(|write| write.operation != "file:write")
                    .collect::<Vec<_>>()
            }),
            driver
                .boundaries
                .iter()
                .rev()
                .filter(|(name, detail)| name == "task:completed" && detail.starts_with("commit "))
                .take(32)
                .collect::<Vec<_>>()
        );
        Ok(())
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
    measure([
        text("one"),
        text("two"),
        text("three"),
        streamed(2),
        streamed(32),
        text("four"),
        text("five"),
    ])
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
    let schedule = driver.schedule.clone();
    let store = LocalCoreStateStore::new(directory.path());
    let result = runtime.block_on(driver.run(entry(async {
        // Ordinary transactions checkpoint after their WAL commit is flushed.
        assert!(
            WorkspaceLineageStore::compare_and_swap(&store, workspace_id, 0, record.clone())
                .await
                .map_err(storage)?
        );
        Ok::<_, Error>(())
    })));
    match result {
        driver::Outcome::Completed(result) => result?,
        driver::Outcome::Crashed(cause) => panic!("uncut checkpoint crashed: {cause}"),
    }
    assert_eq!(
        runtime
            .block_on(WorkspaceLineageStore::load(&store, workspace_id))
            .map_err(storage)?,
        Some(record)
    );
    let writes = schedule.durable_writes();
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
        let start = schedule.durable_write_count();
        runtime
            .block_on(
                schedule.scope(async { acyclic_native_runtime::sync_filesystem(directory.path()) }),
            )
            .map_err(storage)?;
        assert_eq!(
            flushes(&schedule.durable_writes()[start..]),
            (0, 1),
            "the oracle missed the native filesystem-wide drain"
        );
    }
    Ok(())
}
