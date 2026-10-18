use super::{
    TestResult, matchers, quiescent,
    swarm_case::SwarmCase,
    swarm_publish::{self as publish, Producer},
    swarm_views::{Action, State},
    swarm_wire::{self as protocol, Fault, Wire},
};
use acyclic_harness::{
    Error,
    client::{ClientViewLimits, QueryViewLimits, SwarmClient, SwarmClientLimits},
    task_volume::{Query, TaskVolume},
};
use acyclic_stream::MemoryStream;
use futures::{FutureExt as _, StreamExt as _, channel::mpsc};
use std::{future::Future as _, sync::Arc, task::Poll};

pub(super) fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

pub(super) async fn run(case: SwarmCase) -> TestResult {
    run_with_fault(case, Fault::None, false).await.map(|_| ())
}

pub(super) async fn run_with_fault(
    case: SwarmCase,
    fault: Fault,
    paced: bool,
) -> TestResult<serde_json::Value> {
    super::stall::guarded(std::path::Path::new("swarm"), run_raw(case, fault, paced))
        .await
        .map_err(std::io::Error::other)?
}

async fn run_raw(case: SwarmCase, fault: Fault, paced: bool) -> TestResult<serde_json::Value> {
    let started = std::time::Instant::now();
    case.validate()?;
    let provider = Arc::new(MemoryStream::default());
    let parent = TaskVolume::open(provider.clone(), publish::path("swarm/files")).await?;
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
        let status = acyclic_harness::task_volume::ChildStatus {
            task: acyclic_harness::TaskId::from_bytes([1; 16]),
            state: acyclic_harness::task_volume::ChildState::Running,
            settled: false,
            inputs: 1,
        };
        parent
            .append(
                parent.head().await?,
                &acyclic_harness::task_volume::Write::new(
                    acyclic_harness::task_volume::FileKey::parse(&format!(
                        "agents/a{id:05}/status.json"
                    ))?,
                    serde_json::to_vec(&status)?,
                ),
            )
            .await?;
        volumes.push(volume);
    }
    let seeded = started.elapsed();
    let Wire {
        client: session,
        mut runtime,
        sender,
        counts,
    } = Wire::open(provider.clone(), fault, case.agents, paced)?;
    let (phases, receive_phases) = mpsc::unbounded();
    let (receipts, receive_receipts) = mpsc::unbounded();
    let (gates, mut paused) = mpsc::unbounded();
    let (status_receipts, receive_status_receipts) = mpsc::unbounded();
    let mut state = State::new(&case, receipts, fault, paced.then_some(status_receipts));
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
        ClientViewLimits {
            agents: case.agents,
            selected,
            overview_values: case.agents,
            overview_bytes: case.agents as u64 * 256,
            query: QueryViewLimits {
                files: 1,
                segments: records,
                bytes: case
                    .turns
                    .iter()
                    .flatten()
                    .map(|text| text.len() as u64)
                    .sum(),
            },
        },
    )?;
    let views = client.views();
    let consume = client.follow(
        &config,
        &parent,
        &session,
        |agent, maximum| {
            let id = publish::id(&agent).expect("generated agent");
            let volume = volumes[id].clone();
            async move {
                let mut snapshot = volume.heads(&Query::Root, None, maximum).await?;
                if fault == Fault::CorruptSnapshot && id == 0 {
                    // Keep the genuine cut and omit only the initial client bytes.
                    let file = &mut snapshot.files[0];
                    let missing = std::mem::replace(&mut file.segments[0], Vec::new().into());
                    file.length -= missing.len() as u64;
                }
                Ok(snapshot)
            }
        },
        async |update| match state.accept(update, &views)? {
            Action::None => Ok(()),
            Action::Gap { id, sequence, text } => {
                publish::chunk(&volumes[id], sequence, &text).await?;
                Ok(())
            }
            Action::Layout(phase) => publish::layout(&config, &case.layouts[phase]).await,
            Action::Phase(phase) => phases.unbounded_send(phase).map_err(publish::storage),
        },
    );
    let consume = async {
        let driver = runtime.drive();
        futures::pin_mut!(consume, driver);
        match futures::future::select(consume, driver).await {
            futures::future::Either::Left((outcome, _)) => outcome,
            futures::future::Either::Right(_) => {
                Err(Error::Storage("session executor ended".into()))
            }
        }
    };
    // Meter the production subscription pipeline, including the server polled
    // by its incoming stream and snapshot/reducer callbacks. Fixture seeding,
    // producer writes and cold-reopen checks are outside this work boundary.
    let mut allocation = acyclic_test_support::AllocationInfo::default();
    let allocated = &mut allocation;
    let mut consume = Box::pin(consume);
    let consume = futures::future::poll_fn(move |cx| {
        let mut result = Poll::Pending;
        *allocated += acyclic_test_support::measure(|| result = consume.as_mut().poll(cx));
        result
    });
    let produce = Producer {
        case: &case,
        provider: &provider,
        volumes: &volumes,
        phases: receive_phases,
        receipts: receive_receipts,
        status_receipts: paced.then_some(receive_status_receipts),
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
    // Ordinary scenarios retain their initial zero-channel-credit conflation.
    // Scaling scenarios acknowledge every status publication with the same finite
    // window, making delivered histories identical at both population sizes.
    if !paced {
        protocol::release(&sender, &counts)?;
    }
    gate.resume
        .send(())
        .map_err(|_| "producer credit gate ended")?;
    if let Some(result) = quiescent::until_idle(running.as_mut()).await {
        return Err(result
            .err()
            .unwrap_or_else(|| Error::Invalid("swarm detached".into()))
            .into());
    }
    {
        let observed = counts.lock().unwrap_or_else(|p| p.into_inner());
        if observed.peak_full > selected
            || observed.live_full != case.layouts.last().unwrap().selected().count()
        {
            return Err(format!(
                "live server follows exceeded selected bound: peak={} live={} bound={selected}",
                observed.peak_full, observed.live_full
            )
            .into());
        }
    }
    drop(running);
    let mut idle = Box::pin(futures::future::pending::<()>());
    assert!(runtime.until_idle(idle.as_mut()).await.is_none());
    if counts.lock().unwrap_or_else(|p| p.into_inner()).live_full != 0 {
        return Err("server retained follows after consumer cancellation".into());
    }
    drop(runtime);
    drop(session);
    if counts.lock().unwrap_or_else(|p| p.into_inner()).live_full != 0 {
        return Err("server retained follows after session drop".into());
    }
    state.verify()?;
    let mut wire = serde_json::to_value(&*counts.lock().unwrap_or_else(|p| p.into_inner()))?;
    wire.as_object_mut()
        .ok_or("wire counters")?
        .extend(state.report().as_object().ok_or("view counters")?.clone());
    wire["allocations"] = allocation.count_total.into();
    wire["allocated_bytes"] = allocation.bytes_total.into();
    if !matchers::subset(&case.expect.wire, &wire) {
        return Err(format!("swarm wire observations differ: {wire}").into());
    }
    let followed = started.elapsed();
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
    println!(
        "swarm phases agents={} seed={seeded:?} follow={:?} reopen={:?}",
        case.agents,
        followed - seeded,
        started.elapsed() - followed
    );
    measured.sort_unstable();
    if !measured.is_empty() {
        println!(
            "swarm agents={} sessions=1 samples={} tier1_p50_ns={} tier1_p99_ns={} wire={wire}",
            case.agents,
            measured.len(),
            measured[measured.len() / 2],
            measured[(measured.len() * 99 / 100).min(measured.len() - 1)]
        );
    }
    Ok(wire)
}
