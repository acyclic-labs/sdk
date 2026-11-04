//! Crash the shipping local composition, then reopen with no retained agent state.
use super::*;
use acyclic_harness::{
    model::{ModelDispatch, ModelEvent},
    test_support::ScriptedReconciliation,
};
use acyclic_native_runtime::testing::DurableFault;
use driver::{Driver, Outcome};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy)]
enum Cut {
    FirstTurnWrite,
    FirstModelEvent,
}

fn local_turn<'a>(
    directory: &'a Path,
    schedule: &'a acyclic_native_runtime::testing::Schedule,
    cut: Option<Cut>,
    ordinal: Arc<AtomicUsize>,
    provider: Arc<ScriptedModelProvider>,
) -> EntryFuture<'a, Result<()>> {
    entry(async move {
        let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
        let config = local_harness_config(directory.join("checkout"), (model, provider))?
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        if matches!(cut, Some(Cut::FirstTurnWrite)) {
            ordinal.store(schedule.cut_next(DurableFault::After), Ordering::SeqCst);
        }
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
        let directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(directory.path().join("checkout")).map_err(storage)?;
        let mut crash = Driver::new(71, None);
        let schedule = crash.schedule.clone();
        let ordinal = Arc::new(AtomicUsize::new(usize::MAX));
        let dispatched = Arc::new(Mutex::new(None::<ModelDispatch>));
        let original_dispatch = Arc::clone(&dispatched);
        let dispatch_schedule = schedule.clone();
        let dispatch_ordinal = Arc::clone(&ordinal);
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_dispatch_observer(
                move |dispatch| {
                    *original_dispatch
                        .lock()
                        .expect("dispatch observation poisoned") = Some(dispatch);
                    if matches!(point, Cut::FirstModelEvent) {
                        dispatch_ordinal.store(
                            dispatch_schedule.cut_next(DurableFault::After),
                            Ordering::SeqCst,
                        );
                    }
                    Box::pin(async { Ok(()) })
                },
            ),
        );
        let Outcome::Crashed(cause) = crash
            .run(local_turn(
                directory.path(),
                &schedule,
                Some(point),
                Arc::clone(&ordinal),
                provider,
            ))
            .await
        else {
            panic!("turn escaped its durable write cut");
        };
        assert_eq!(cause.ordinal, ordinal.load(Ordering::SeqCst));
        assert_eq!(cause.fault, DurableFault::After);
        let dispatched = *dispatched.lock().expect("dispatch observation poisoned");
        assert_eq!(dispatched.is_some(), matches!(point, Cut::FirstModelEvent));
        drop(schedule);
        drop(crash);
        // Independent providers, stores, clocks, identity source and executor
        // reopen the original files. The provider explicitly recovers the
        // previously accepted request; no old provider object crosses the cut.
        let mut recovery = Driver::new(72, None);
        let schedule = recovery.schedule.clone();
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_reconciliations(
                dispatched.map(|_| ScriptedReconciliation {
                    orphan: true,
                    observed: vec![],
                    continuation: Some(vec![
                        ModelEvent::Content {
                            delta: "done".into(),
                        },
                        ModelEvent::Completed {
                            metadata: serde_json::Value::Null,
                        },
                    ]),
                }),
            ),
        );
        let result = match recovery
            .run(local_turn(
                directory.path(),
                &schedule,
                None,
                Arc::new(AtomicUsize::new(usize::MAX)),
                Arc::clone(&provider),
            ))
            .await
        {
            Outcome::Completed(result) => result,
            Outcome::Crashed(cause) => panic!("uncut recovery crashed: {cause}"),
        };
        result?;
        assert_eq!(
            provider.reconciliations(),
            dispatched.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(provider.request_count(), usize::from(dispatched.is_none()));
        Ok(())
    })
}

#[test]
fn checkout_reopens_after_first_turn_write() -> Result<()> {
    crash_reopen(Cut::FirstTurnWrite)
}

#[test]
fn live_f3_killed_head_accepts_a_new_prompt() -> Result<()> {
    crash_reopen(Cut::FirstModelEvent)
}
