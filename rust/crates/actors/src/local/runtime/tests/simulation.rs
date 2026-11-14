//! Random operation histories over the real local delivery loop.
use super::*;
use acyclic_native_runtime::testing::{Schedule, fill_seeded, seeded_runtime};

#[derive(Default)]
struct Expected {
    sent: u64,
    cursor: u64,
    paused: bool,
    pending_records: Vec<u64>,
    records: Vec<String>,
    alarms: Vec<String>,
    generation: u64,
    deadline: Option<u64>,
    delivered: Option<u64>,
}

async fn open(
    store: &Store<MemoryStream>,
    addresses: &[ActorReference],
) -> Result<Runtime<MemoryStream>, Box<dyn std::error::Error>> {
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    for address in addresses {
        let (index, _) = runtime.placement_state(address).await?;
        runtime.place(address.clone(), index + 1).await?;
    }
    Ok(runtime)
}

async fn history(schedule: &Schedule) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(provider.clone(), root()?);
    let addresses = [
        spawn_with(&store, "history-first", &[b"emit"]).await?,
        spawn_with(&store, "history-second", &[b"emit"]).await?,
        spawn_with(&store, "history-third", &[b"emit"]).await?,
    ];
    let mut runtime = open(&store, &addresses).await?;
    let mut expected: [Expected; 3] = std::array::from_fn(|_| Expected {
        sent: 1,
        pending_records: vec![0],
        ..Expected::default()
    });
    let mut now = SystemClock.now_unix_ms();
    for step in 0..48 {
        let mut choices = [0; 4];
        assert!(fill_seeded(&mut choices));
        let index = usize::from(choices[0]) % addresses.len();
        let address = &addresses[index];
        let model = &mut expected[index];
        match choices[1] % 7 {
            0 => {
                // Drop all volatile placement and cursor caches. The next
                // owner must recover solely from the provider's committed cut.
                drop(runtime);
                runtime = open(&store, &addresses).await?;
            }
            1 => {
                now += u64::from(choices[2] % 20) + 1;
                schedule.set_unix_millis(now);
            }
            2 => {
                model.paused = !model.paused;
                runtime.pause(address, model.paused).await?;
            }
            action => {
                // A parked actor accumulates ordinary input; alarm controls
                // are sent only at a quiescent delivery cut.
                let command = match (action, model.paused) {
                    (3, false) => {
                        model.generation += 1;
                        model.deadline = Some(now + 10);
                        model.delivered = None;
                        "set 10"
                    }
                    (4, false) => {
                        model.generation += 1;
                        model.deadline = None;
                        model.delivered = None;
                        "clear"
                    }
                    _ => {
                        model.pending_records.push(model.sent);
                        "emit"
                    }
                };
                provider
                    .append(AppendRequest {
                        path: StreamPath::new(format!("agents/{}", address.id()))?,
                        records: vec![Bytes::copy_from_slice(command.as_bytes()).into()],
                        if_tail: Some(model.sent),
                        idempotency_key: key(&format!("history/{step}"))?,
                    })
                    .await?;
                model.sent += 1;
            }
        }
        // Independent actors run concurrently; their conditional publications
        // and shared provider remain the production implementations.
        futures::future::try_join_all(addresses.iter().map(|address| runtime.drain_actor(address)))
            .await?;
        for (address, model) in addresses.iter().zip(&mut expected) {
            if !model.paused {
                model.cursor = model.sent;
                model.records.extend(
                    model
                        .pending_records
                        .drain(..)
                        .map(|sequence| format!("r{sequence}")),
                );
                if model.deadline.is_some_and(|deadline| deadline <= now)
                    && model.delivered != Some(model.generation)
                {
                    model.alarms.push(format!("a{}", model.generation));
                    model.delivered = Some(model.generation);
                }
            }
            let snapshot = store.load(address).await?;
            let state = snapshot.state().ok_or("missing actor")?;
            assert_eq!(state.cursor("inbox"), Some(model.cursor), "step={step}");
            assert_eq!(state.paused(), model.paused, "step={step}");
            assert_eq!(state.alarm().generation(), model.generation, "step={step}");
            assert_eq!(
                state.alarm().deadline_unix_ms(),
                model.deadline,
                "step={step}"
            );
            assert_eq!(
                state.alarm().delivered_generation,
                model.delivered,
                "step={step}"
            );
            let output = records(provider.as_ref(), &format!("out/{}", address.id())).await?;
            let output = output
                .iter()
                .map(|bytes| std::str::from_utf8(bytes))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(
                output
                    .iter()
                    .copied()
                    .filter(|value| value.starts_with('r'))
                    .collect::<Vec<_>>(),
                model.records,
                "step={step}"
            );
            assert_eq!(
                output
                    .iter()
                    .copied()
                    .filter(|value| value.starts_with('a'))
                    .collect::<Vec<_>>(),
                model.alarms,
                "step={step}"
            );
        }
    }
    Ok(())
}

fn replay(seed: u64) -> TestResult {
    eprintln!("seed={seed}");
    seeded_runtime(seed)?.block_on(async {
            let (schedule, mut boundaries) = Schedule::seeded(seed);
            let turn = schedule.scope(history(&schedule));
            tokio::pin!(turn);
            loop {
                tokio::select! {
                    result = &mut turn => return result.map_err(|error| format!("seed={seed}: {error}").into()),
                    boundary = boundaries.recv() => boundary.ok_or("boundary channel closed")?.release(),
                }
            }
    })
}

#[test]
fn seeded_runtime_delivery_alarms_pause_and_restart() -> TestResult {
    for seed in [0, 1, 7, 42] {
        replay(seed)?;
    }
    Ok(())
}

#[test]
#[ignore = "deep seeded sweep; run on an idle worker"]
fn deep_runtime_delivery_alarms_pause_and_restart() -> TestResult {
    for seed in 0..4096 {
        replay(seed)?;
    }
    Ok(())
}
