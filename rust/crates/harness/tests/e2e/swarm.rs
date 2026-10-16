use super::{
    TestResult, matchers, quiescent,
    swarm_case::SwarmCase,
    swarm_publish::{self as publish, Producer},
    swarm_views::{Action, State},
    swarm_wire::{self as protocol, Fault, Wire},
};
use acyclic_harness::{
    Error,
    client::{SwarmClient, SwarmClientLimits},
    task_volume::{Query, TaskVolume},
};
use acyclic_stream::MemoryStream;
use futures::{FutureExt as _, StreamExt as _, channel::mpsc};
use std::sync::Arc;

pub(super) async fn run(case: SwarmCase) -> TestResult {
    run_with_fault(case, Fault::None).await
}

pub(super) async fn run_with_fault(case: SwarmCase, fault: Fault) -> TestResult {
    case.validate()?;
    let provider = Arc::new(MemoryStream::default());
    let config = Arc::new(TaskVolume::open(provider.clone(), publish::path("config/files")).await?);
    publish::layout(&config, &case.layouts[0]).await?;
    let mut volumes = Vec::with_capacity(case.agents);
    for id in 0..case.agents {
        let volume = Arc::new(
            TaskVolume::open(
                provider.clone(),
                publish::path(&format!("{}/files", publish::agent(id).as_str())),
            )
            .await?,
        );
        publish::chunk(&volume, 1, &case.turns[0][0]).await?;
        publish::publish_status(&provider, id, publish::status(0, "idle", 1), false).await?;
        volumes.push(volume);
    }
    let Wire {
        sink,
        incoming,
        sender,
        counts,
    } = Wire::open(provider.clone(), fault)?;
    let (phases, receive_phases) = mpsc::unbounded();
    let (receipts, receive_receipts) = mpsc::unbounded();
    let (gates, mut paused) = mpsc::unbounded();
    let mut state = State::new(&case, receipts);
    let mut measured = Vec::new();
    let records = case.turns.iter().map(Vec::len).sum::<usize>();
    let selected = case
        .layouts
        .iter()
        .map(|layout| layout.selected().count())
        .max()
        .unwrap();
    let client = SwarmClient::new(
        publish::path("swarm"),
        SwarmClientLimits {
            agents: case.agents,
            selected,
            layout_bytes: 16_384,
            config_files: 1,
            snapshot_files: records,
        },
    )?;
    let consume = client.follow(
        provider.as_ref(),
        &config,
        sink,
        incoming,
        |agent, maximum| {
            let volume = volumes[publish::id(&agent).expect("generated agent")].clone();
            async move { volume.heads(&Query::Root, None, maximum).await }
        },
        |update| {
            let action = state.accept(update);
            let volumes = &volumes;
            let config = &config;
            let layouts = &case.layouts;
            let phases = &phases;
            async move {
                match action? {
                    Action::None => Ok(()),
                    Action::Gap { id, sequence, text } => {
                        publish::chunk(&volumes[id], sequence, &text).await?;
                        Ok(())
                    }
                    Action::Layout(phase) => publish::layout(config, &layouts[phase]).await,
                    Action::Phase(phase) => phases.unbounded_send(phase).map_err(publish::storage),
                }
            }
        },
    );
    let produce = Producer {
        case: &case,
        provider: &provider,
        volumes: &volumes,
        phases: receive_phases,
        receipts: receive_receipts,
        gates,
        measured: &mut measured,
    }
    .run();
    let mut running = Box::pin(tokio::task::unconstrained(futures::future::try_join(
        consume, produce,
    )));
    if let Some(result) = quiescent::until_idle(running.as_mut()).await {
        return Err(result
            .err()
            .unwrap_or_else(|| Error::Invalid("swarm detached".into()))
            .into());
    }
    let gate = paused
        .next()
        .now_or_never()
        .flatten()
        .ok_or("swarm did not reach its credit gate")?;
    if gate.phase != 0 {
        return Err("swarm credit phase differs".into());
    }
    // One finite receive window covers the declared history. Per-channel
    // credits still advance on each record; the initial zero-credit phase
    // exercises backpressure without a global gate transition per delta.
    let frames =
        case.agents * (2 + 4 * case.layouts.len()) + records * selected * case.layouts.len();
    {
        let mut counts = counts.lock().unwrap_or_else(|p| p.into_inner());
        if counts.root_credit != 0 {
            return Err("backpressure did not exhaust root credit".into());
        }
        counts.root_credit = frames;
    }
    protocol::root_window(&sender, frames as u32)?;
    gate.resume
        .send(())
        .map_err(|_| "producer credit gate ended")?;
    if let Some(result) = quiescent::until_idle(running.as_mut()).await {
        return Err(result
            .err()
            .unwrap_or_else(|| Error::Invalid("swarm detached".into()))
            .into());
    }
    drop(running);
    state.verify()?;
    let mut wire = serde_json::to_value(&*counts.lock().unwrap_or_else(|p| p.into_inner()))?;
    wire.as_object_mut()
        .ok_or("wire counters")?
        .extend(state.report().as_object().ok_or("view counters")?.clone());
    if !matchers::subset(&case.expect.wire, &wire) {
        return Err(format!("swarm wire observations differ: {wire}").into());
    }
    // Reopen every index from accepted file records, including unselected agents.
    for volume in &volumes {
        let reopened = TaskVolume::open_existing(provider.clone(), volume.path().clone()).await?;
        let snapshot = reopened.heads(&Query::Root, None, records).await?;
        let [file] = snapshot.files.as_slice() else {
            return Err("final agent file missing".into());
        };
        let actual: Vec<_> = file
            .segments
            .iter()
            .flat_map(|s| s.iter())
            .copied()
            .collect();
        if actual != case.expect.text.as_bytes()
            || snapshot.cursor.position != case.through(case.layouts.len() - 1) + 1
        {
            return Err("reopened agent file differs".into());
        }
    }
    measured.sort_unstable();
    if !measured.is_empty() {
        println!(
            "swarm agents={} sessions=1 samples={} tier1_p99_ns={} wire={wire}",
            case.agents,
            measured.len(),
            measured[(measured.len() * 99 / 100).min(measured.len() - 1)]
        );
    }
    Ok(())
}
