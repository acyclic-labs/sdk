//! Device flushes per settled reply turn, counted at the original file calls.
//! A steady reply turn buys exactly three, each for a public guarantee:
//! - one ordering barrier: the turn's object bytes precede every log frame
//!   that names them, so no durable frame names missing bytes;
//! - one drain of a filesystem authority publication at the turn's end, so
//!   the generation the settlement names is durable before it is named;
//! - one drain of the settlement commit, the turn's acknowledgement.
//!
//! Model events, payloads and journal cuts inside the turn add none: the
//! dispatch phase defers them to its barrier. The count is exact, so a new
//! per-event or per-payload flush, or a lost acknowledgement drain, fails.
use super::*;
use acyclic_harness::{model::ModelEvent, test_support::ScriptedEvent};

/// (ordering barriers, device drains) of one steady text reply turn.
const REPLY_TURN_FLUSHES: (usize, usize) = (1, 2);

fn flushes(writes: &[DurableWrite]) -> (usize, usize) {
    let barriers = writes
        .iter()
        .filter(|write| write.operation.ends_with("_barrier"))
        .count();
    let drains = writes
        .iter()
        .filter(|write| {
            matches!(
                write.operation,
                "file:sync" | "file:sync_data" | "file:sync_all" | "file:sync_parent"
            )
        })
        .count();
    (barriers, drains)
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
        let mut cuts = [0; 3];
        for (turn, expected) in expected.iter().enumerate() {
            if turn > 0 {
                cuts[turn - 1] = schedule.durable_writes().len();
            }
            prompt(&harness, &format!("turn-{turn}"), "reply", expected).await?;
        }
        cuts[2] = schedule.durable_writes().len();
        if extra {
            use acyclic_stream::StreamProviderExt as _;
            let (agents, _) = entry(harness.agents()).await?;
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
        Ok(cuts)
    })
}

/// Flushes of the second and third settled turns; the third window runs to
/// the end of all accepted work. Every physical boundary is released at once.
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
            flushes(&writes[cuts[1]..]),
        ])
    })
}

fn text(reply: &str) -> ScriptedTurn {
    ScriptedTurn::Text(reply.into())
}

#[test]
fn a_reply_turn_flushes_once_at_its_settlement() -> Result<()> {
    let [second, third] = measure([text("one"), text("two"), text("three")], false)?;
    // History length never adds a flush.
    assert_eq!(second, third, "reply-turn flushes grew with history");
    assert_eq!(third, REPLY_TURN_FLUSHES, "reply-turn flushes changed");
    Ok(())
}

#[test]
fn negative_control_one_more_durable_commit_is_counted() -> Result<()> {
    let [_, third] = measure([text("one"), text("two"), text("three")], true)?;
    assert_eq!(
        third,
        (REPLY_TURN_FLUSHES.0, REPLY_TURN_FLUSHES.1 + 1),
        "the oracle missed an eager commit's drain"
    );
    Ok(())
}

#[test]
fn streamed_events_add_no_flush_to_a_reply_turn() -> Result<()> {
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
    let [_, short] = measure([text("one"), text("two"), streamed(2)], false)?;
    let [_, long] = measure([text("one"), text("two"), streamed(32)], false)?;
    assert_eq!(short, long, "each streamed event paid its own flush");
    assert_eq!(long, REPLY_TURN_FLUSHES);
    Ok(())
}
