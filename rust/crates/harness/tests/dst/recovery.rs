//! Crash the shipping local composition, then reopen with no retained agent state.
use super::*;
use acyclic_harness::test_support::ScriptedReconciliation;
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
    prompt_id: &'a str,
) -> EntryFuture<'a, Result<Arc<ScriptedModelProvider>>> {
    entry(async move {
        let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
        let dispatch_schedule = schedule.clone();
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())])
                .with_reconciliations([ScriptedReconciliation {
                    orphan: true,
                    observed: Vec::new(),
                    continuation: None,
                }])
                .with_dispatch_observer(move |_| {
                    dispatch_write.store(dispatch_schedule.durable_write_count(), Ordering::SeqCst);
                    Box::pin(async { Ok(()) })
                }),
        );
        let config = local_harness_config(directory.join("checkout"), (model, provider.clone()))?
            .without_user_settings()
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        first_turn_write.set(Some(schedule.durable_write_count()));
        prompt(&harness, prompt_id, "finish this turn", "done").await?;
        Ok(provider)
    })
}

fn crash_reopen(point: Cut) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let baseline_directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(baseline_directory.path().join("checkout")).map_err(storage)?;
        let mut baseline = Driver::new(71, None);
        let marker = Cell::new(None);
        let schedule = baseline.schedule.clone();
        let dispatched = Arc::new(AtomicUsize::new(usize::MAX));
        match baseline
            .run(local_turn(
                baseline_directory.path(),
                &schedule,
                &marker,
                dispatched.clone(),
                "first",
            ))
            .await
        {
            Outcome::Completed(result) => drop(result?),
            Outcome::Crashed(cause) => panic!("uncut local turn crashed: {cause}"),
        }
        let start = marker.get().expect("local turn was never admitted");
        let dispatched = dispatched.load(Ordering::SeqCst);
        assert_ne!(dispatched, usize::MAX, "provider dispatch was bypassed");
        let cut = match point {
            Cut::FirstTurnWrite => start,
            Cut::DispatchIntent => dispatched
                .checked_sub(1)
                .expect("missing durable dispatch intent"),
        };
        assert!(cut >= start, "cut precedes turn admission");
        assert!(
            baseline.schedule.durable_write_count() > cut,
            "turn made no durable writes"
        );
        drop(baseline);
        drop(schedule);
        let directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(directory.path().join("checkout")).map_err(storage)?;
        let mut crash = Driver::new(71, Some((cut, DurableFault::After)));
        let schedule = crash.schedule.clone();
        let marker = Cell::new(None);
        let Outcome::Crashed(cause) = crash
            .run(local_turn(
                directory.path(),
                &schedule,
                &marker,
                Arc::new(AtomicUsize::new(usize::MAX)),
                "first",
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
        let mut recovery = Driver::new(72, None);
        let schedule = recovery.schedule.clone();
        let marker = Cell::new(None);
        let provider = match recovery
            .run(local_turn(
                directory.path(),
                &schedule,
                &marker,
                Arc::new(AtomicUsize::new(usize::MAX)),
                "second",
            ))
            .await
        {
            Outcome::Completed(result) => result?,
            Outcome::Crashed(cause) => panic!("uncut recovery crashed: {cause}"),
        };
        assert!(
            marker.get().is_some(),
            "workspace did not reopen before the new prompt"
        );
        // A fresh provider cannot recover the killed request. Its terminal
        // refusal must release that turn before a distinct prompt dispatches.
        assert_eq!(
            provider.request_count(),
            1,
            "regenerated the killed request"
        );
        let reconciled = provider.reconciliations();
        assert_eq!(
            reconciled.len(),
            usize::from(matches!(point, Cut::DispatchIntent)),
            "reconciled a different durable cut"
        );
        if let Some(interrupted) = reconciled.first() {
            assert_ne!(
                interrupted.operation_id,
                provider.dispatches()[0].operation_id,
                "the new prompt reused the interrupted operation"
            );
        }
        Ok(())
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
