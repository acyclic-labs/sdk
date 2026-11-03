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
            );
            (barriers, drains + usize::from(drain))
        }
    })
}

/// Opens a harness and settles three reply turns, returning the durable
/// trace length before the second and third turns and after the third. With
/// `extra`, the last window also holds one eagerly durable commit on the
/// harness's own log.
fn turns(
    schedule: acyclic_native_runtime::testing::Schedule,
    directory: &Path,
    replies: [ScriptedTurn; 3],
    extra: bool,
) -> EntryFuture<'_, Result<[usize; 3]>> {
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
        let config = local_harness_config(directory.join("filesystem"), (model, provider))?
            .volume(volume)
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        let (agents, root) = entry(harness.agents()).await?;
        let mut cuts = [0; 3];
        for (turn, expected) in expected.iter().enumerate() {
            if turn > 0 {
                cuts[turn - 1] = schedule.durable_writes().len();
            }
            prompt(&harness, &format!("turn-{turn}"), "reply", expected).await?;
            // The reply is observable before its serving delivery retires.
            // Wait on that same actor's delivery lock and committed checkpoint,
            // while its ordinary continuous runtime remains running.
            entry(agents.drain(root)).await?;
        }
        if extra {
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
        }
        cuts[2] = schedule.durable_writes().len();
        Ok(cuts)
    })
}

/// Matching second and third settlement windows. Teardown still drains every
/// accepted worker, but belongs to neither reply. Release each original boundary.
fn measure(replies: [ScriptedTurn; 3], extra: bool) -> Result<[(usize, usize); 2]> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        let directory = tempfile::tempdir().map_err(storage)?;
        let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
        let releaser = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        let cuts = schedule
            .scope(turns(schedule.clone(), directory.path(), replies, extra))
            .await?;
        schedule.drained().await;
        releaser.abort();
        let writes = schedule.durable_writes();
        Ok([
            flushes(&writes[cuts[0]..cuts[1]]),
            flushes(&writes[cuts[1]..cuts[2]]),
        ])
    })
}

fn text(reply: &str) -> ScriptedTurn {
    ScriptedTurn::Text(reply.into())
}

#[test]
fn reply_publication_drains_follow_commits_and_not_history_or_event_count() -> Result<()> {
    let [second, third] = measure([text("one"), text("two"), text("three")], false)?;
    // History length never adds a flush.
    assert_eq!(second, third, "reply-turn flushes grew with history");
    let [_, extra] = measure([text("one"), text("two"), text("three")], true)?;
    assert_eq!(
        extra,
        (third.0, third.1 + 1),
        "the oracle missed an eager commit's drain"
    );
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
    let [short, long] = measure([text("one"), streamed(2), streamed(32)], false)?;
    assert_eq!(short.1, long.1, "each streamed event paid its own drain");
    assert_eq!(
        short.0 + 30,
        long.0,
        "each additional private payload needs one publication ordering barrier"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn negative_control_filesystem_wide_flush_is_counted() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    std::fs::write(directory.path().join("dirty"), b"durable").map_err(storage)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(storage)?;
    let (schedule, _) = acyclic_native_runtime::testing::Schedule::seeded(71);
    runtime
        .block_on(
            schedule.scope(async { acyclic_native_runtime::sync_filesystem(directory.path()) }),
        )
        .map_err(storage)?;
    assert_eq!(
        flushes(&schedule.durable_writes()),
        (0, 1),
        "the oracle missed the native filesystem-wide drain"
    );
    Ok(())
}
