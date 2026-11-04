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
}

fn local_turn<'a>(
    directory: &'a Path,
    schedule: &'a acyclic_native_runtime::testing::Schedule,
    first_turn_write: &'a Cell<Option<usize>>,
    dispatch_write: Arc<AtomicUsize>,
    cut_first_write: bool,
) -> EntryFuture<'a, Result<()>> {
    entry(async move {
        let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
        let dispatch_schedule = schedule.clone();
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_dispatch_observer(
                move |_| {
                    dispatch_write.store(dispatch_schedule.durable_write_count(), Ordering::SeqCst);
                    Box::pin(async { Ok(()) })
                },
            ),
        );
        let config = local_harness_config(directory.join("checkout"), (model, provider))?
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        first_turn_write.set(Some(if cut_first_write {
            schedule.cut_next(DurableFault::After)
        } else {
            schedule.durable_write_count()
        }));
        prompt(&harness, "first", "finish this turn", "done").await?;
        Ok(())
    })
}

fn crash_reopen(point: Cut) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let baseline = match point {
            Cut::FirstTurnWrite => None,
            Cut::DispatchIntent => {
                let directory = tempfile::tempdir().map_err(storage)?;
                std::fs::create_dir(directory.path().join("checkout")).map_err(storage)?;
                let mut baseline = Driver::new(71, None);
                let marker = Cell::new(None);
                let schedule = baseline.schedule.clone();
                let dispatched = Arc::new(AtomicUsize::new(usize::MAX));
                match baseline
                    .run(local_turn(
                        directory.path(),
                        &schedule,
                        &marker,
                        dispatched.clone(),
                        false,
                    ))
                    .await
                {
                    Outcome::Completed(result) => result?,
                    Outcome::Crashed(cause) => panic!("uncut local turn crashed: {cause}"),
                }
                let start = marker.get().expect("local turn was never admitted");
                let dispatched = dispatched.load(Ordering::SeqCst);
                assert_ne!(dispatched, usize::MAX, "provider dispatch was bypassed");
                let cut = dispatched
                    .checked_sub(1)
                    .expect("missing durable dispatch intent");
                assert!(cut >= start, "cut precedes turn admission");
                assert!(
                    schedule.durable_write_count() > cut,
                    "turn made no durable writes"
                );
                Some((start, cut))
            }
        };
        let directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(directory.path().join("checkout")).map_err(storage)?;
        let mut crash = Driver::new(71, baseline.map(|(_, cut)| (cut, DurableFault::After)));
        let schedule = crash.schedule.clone();
        let marker = Cell::new(None);
        let cause = match crash
            .run(local_turn(
                directory.path(),
                &schedule,
                &marker,
                Arc::new(AtomicUsize::new(usize::MAX)),
                matches!(point, Cut::FirstTurnWrite),
            ))
            .await
        {
            Outcome::Crashed(cause) => cause,
            Outcome::Completed(result) => {
                result?;
                panic!("turn escaped its durable write cut");
            }
        };
        assert_eq!(cause.fault, DurableFault::After);
        if let Some((start, cut)) = baseline {
            assert_eq!(cause.ordinal, cut);
            assert_eq!(marker.get(), Some(start), "crashed a different operation");
        } else {
            assert_eq!(
                marker.get(),
                Some(cause.ordinal),
                "crashed a different operation"
            );
        }
        drop(schedule);
        drop(crash);
        // This epoch derives from the same seed. Its providers, stores, clocks,
        // identity source and executor are constructed independently.
        let mut recovery = Driver::new(72, None);
        let schedule = recovery.schedule.clone();
        let marker = Cell::new(None);
        let result = match recovery
            .run(local_turn(
                directory.path(),
                &schedule,
                &marker,
                Arc::new(AtomicUsize::new(usize::MAX)),
                false,
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
        result
    })
}

#[test]
fn checkout_reopens_after_first_turn_write() -> Result<()> {
    crash_reopen(Cut::FirstTurnWrite)
}

#[test]
fn live_f3_killed_head_accepts_a_new_prompt() -> Result<()> {
    crash_reopen(Cut::DispatchIntent)
}
