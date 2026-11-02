//! Crash the shipping local composition, then reopen with no retained agent state.
use super::*;
use acyclic_native_runtime::testing::DurableFault;
use driver::{Driver, Outcome};
use std::{
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy)]
enum Cut {
    FirstTurnWrite,
    DispatchIntent,
    /// Every ordering barrier and device drain of the turn: dispatch intent,
    /// generation, terminal observation and settlement commit.
    EveryDurabilityPoint,
}

fn local_turn<'a>(
    directory: &'a Path,
    schedule: &'a acyclic_native_runtime::testing::Schedule,
    first_turn_write: &'a Cell<Option<usize>>,
    dispatch_write: Arc<AtomicUsize>,
) -> EntryFuture<'a, Result<()>> {
    entry(async move {
        let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
        let dispatch_schedule = schedule.clone();
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_dispatch_observer(
                move |_| {
                    dispatch_write
                        .store(dispatch_schedule.durable_writes().len(), Ordering::SeqCst);
                },
            ),
        );
        let config = local_harness_config(directory.join("checkout"), (model, provider))?
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        first_turn_write.set(Some(schedule.durable_writes().len()));
        prompt(&harness, "first", "finish this turn", "done").await?;
        Ok(())
    })
}

/// Crashes the turn at each cut of `point` and reopens it, returning every
/// recovery's result.
fn crash_reopen(point: Cut, seeded: bool) -> Result<Vec<Result<()>>> {
    let driver = |seed, cut| {
        if seeded {
            Driver::new(seed, cut)
        } else {
            Driver::unseeded(cut)
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        if seeded {
            tokio::time::pause();
        }
        let baseline_directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(baseline_directory.path().join("checkout")).map_err(storage)?;
        let mut baseline = driver(71, None);
        let marker = Cell::new(None);
        let schedule = baseline.schedule.clone();
        let dispatched = Arc::new(AtomicUsize::new(usize::MAX));
        match baseline
            .run(local_turn(
                baseline_directory.path(),
                &schedule,
                &marker,
                dispatched.clone(),
            ))
            .await
        {
            Outcome::Completed(result) => result?,
            Outcome::Crashed(cause) => panic!("uncut local turn crashed: {cause}"),
        }
        let start = marker.get().expect("local turn was never admitted");
        let dispatched = dispatched.load(Ordering::SeqCst);
        assert_ne!(dispatched, usize::MAX, "provider dispatch was bypassed");
        let writes = baseline.schedule.durable_writes();
        let cuts: Vec<usize> = match point {
            Cut::FirstTurnWrite => vec![start],
            Cut::DispatchIntent => vec![
                dispatched
                    .checked_sub(1)
                    .expect("missing durable dispatch intent"),
            ],
            Cut::EveryDurabilityPoint => writes[start..]
                .iter()
                .filter(|write| write.operation.starts_with("file:sync"))
                .map(|write| write.ordinal)
                .collect(),
        };
        assert!(!cuts.is_empty(), "turn crossed no durability point");
        for &cut in &cuts {
            assert!(cut >= start, "cut precedes turn admission");
            assert!(writes.len() > cut, "turn made no durable writes");
        }
        drop(writes);
        drop(baseline);
        drop(schedule);
        let mut results = Vec::with_capacity(cuts.len());
        for cut in cuts {
            let directory = tempfile::tempdir().map_err(storage)?;
            std::fs::create_dir(directory.path().join("checkout")).map_err(storage)?;
            let mut crash = driver(71, Some((cut, DurableFault::After)));
            let schedule = crash.schedule.clone();
            let marker = Cell::new(None);
            let Outcome::Crashed(cause) = crash
                .run(local_turn(
                    directory.path(),
                    &schedule,
                    &marker,
                    Arc::new(AtomicUsize::new(usize::MAX)),
                ))
                .await
            else {
                panic!("turn escaped its durable write cut");
            };
            assert_eq!(cause.ordinal, cut);
            assert_eq!(cause.fault, DurableFault::After);
            assert_eq!(marker.get(), Some(start), "crashed a different operation");
            drop(schedule);
            drop(crash);
            // This epoch derives from the same seed. Its providers, stores, clocks,
            // identity source and executor are constructed independently.
            let mut recovery = driver(72, None);
            let schedule = recovery.schedule.clone();
            let marker = Cell::new(None);
            let result = match recovery
                .run(local_turn(
                    directory.path(),
                    &schedule,
                    &marker,
                    Arc::new(AtomicUsize::new(usize::MAX)),
                ))
                .await
            {
                Outcome::Completed(result) => result,
                Outcome::Crashed(cause) => panic!("uncut recovery crashed: {cause}"),
            };
            assert!(
                marker.get().is_some(),
                "workspace did not reopen before the new prompt"
            );
            results.push(result);
        }
        Ok(results)
    })
}

/// A crashed turn either completes on reopen or settles with a definite
/// interruption; it is never stranded as uncertain.
fn settled(result: &Result<()>) -> bool {
    match result {
        Ok(()) => true,
        Err(error) => error.to_string().contains("connection interrupted"),
    }
}

#[test]
fn checkout_reopens_after_first_turn_write() -> Result<()> {
    crash_reopen(Cut::FirstTurnWrite, true)?
        .into_iter()
        .collect()
}

#[test]
fn live_f3_killed_head_accepts_a_new_prompt() -> Result<()> {
    crash_reopen(Cut::DispatchIntent, true)?
        .into_iter()
        .collect()
}

#[test]
fn a_turn_crashed_at_any_durability_point_settles_on_reopen() -> Result<()> {
    for result in crash_reopen(Cut::EveryDurabilityPoint, false)? {
        assert!(settled(&result), "crashed turn was stranded: {result:?}");
    }
    Ok(())
}

#[test]
fn negative_control_a_stranding_resume_is_detected() -> Result<()> {
    acyclic_harness::executor::unsettled_resume::set(true);
    let results = crash_reopen(Cut::EveryDurabilityPoint, false);
    acyclic_harness::executor::unsettled_resume::set(false);
    assert!(
        results?.iter().any(|result| !settled(result)),
        "the oracle accepted a stranded resume"
    );
    Ok(())
}
