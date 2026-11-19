//! Crash the shipping local composition, then reopen with no retained agent state.
use super::*;
use acyclic_harness::{
    model::{ModelDispatch, ModelEvent},
    test_support::{ScriptedReconciliation, is_retrospective},
};
use acyclic_native_runtime::testing::DurableFault;
use driver::{Driver, Outcome};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Cut {
    FirstTurnWrite,
    FirstModelEvent,
    /// The first model event of the settled turn's feedback retrospective,
    /// after its answer is journaled and before the turn settles.
    Retrospective,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum SeedVersion {
    #[serde(rename = "1")]
    V1,
}

/// Inputs to the shipping local composition, not a guessed failed receipt.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaySeed {
    version: SeedVersion,
    scheduler: u64,
    recovery_scheduler: u64,
    cut: Cut,
    first_prompt: String,
    next_prompt: String,
    response: String,
}

fn seed_error(cause: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::exchange(
        acyclic_harness::ExchangeStep::Step("DST recovery seed"),
        cause,
    )
}

impl ReplaySeed {
    fn original(cut: Cut) -> Self {
        Self {
            version: SeedVersion::V1,
            scheduler: 71,
            recovery_scheduler: 72,
            cut,
            first_prompt: "finish this turn".into(),
            next_prompt: "submit after recovery".into(),
            response: "done".into(),
        }
    }

    fn read(path: &Path) -> Result<Self> {
        serde_json::from_slice(&std::fs::read(path).map_err(seed_error)?).map_err(seed_error)
    }

    fn retain(&self, directory: &Path) -> Result<std::path::PathBuf> {
        let path = directory.join("replay-seed.json");
        let next = directory.join("replay-seed.json.next");
        let bytes = serde_json::to_vec(self).map_err(seed_error)?;
        acyclic_native_runtime::private_file::publish(
            &path,
            &next,
            acyclic_native_runtime::RenameMode::NoReplace,
            |file| std::io::Write::write_all(file, &bytes),
            |_| Ok(()),
        )
        .map_err(seed_error)?;
        Ok(path)
    }
}
fn local_turn<'a>(
    directory: &'a Path,
    schedule: &'a acyclic_native_runtime::testing::Schedule,
    cut: Option<Cut>,
    ordinal: Arc<AtomicUsize>,
    provider: Arc<ScriptedModelProvider>,
    seed: &'a ReplaySeed,
) -> EntryFuture<'a, Result<()>> {
    entry(async move {
        let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
        let config = local_harness_config(directory.join("checkout"), (model, provider))?
            .without_user_settings()
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        if matches!(cut, Some(Cut::FirstTurnWrite)) {
            ordinal.store(schedule.cut_next(DurableFault::After), Ordering::SeqCst);
        }
        prompt(&harness, "first", &seed.first_prompt, &seed.response).await?;
        if cut.is_none() {
            prompt(&harness, "second", &seed.next_prompt, &seed.response).await?;
        }
        Ok(())
    })
}

fn crash_reopen(point: Cut) -> Result<()> {
    let seed = match std::env::var_os("SDK_DST_REPLAY_SEED") {
        Some(path) => ReplaySeed::read(Path::new(&path))?,
        None => ReplaySeed::original(point),
    };
    let runtime =
        acyclic_native_runtime::testing::seeded_runtime(seed.scheduler).map_err(seed_error)?;
    runtime.block_on(async {
        tokio::time::pause();
        // The controller retains the original world from before execution, so
        // failure/unwind cannot delete its journal through TempDir cleanup.
        let directory = temporary_directory().map_err(seed_error)?.keep();
        let seed_path = seed.retain(&directory)?;
        println!("DST_REPLAY_SEED={}", seed_path.display());
        let seed = ReplaySeed::read(&seed_path)?;
        let point = seed.cut;
        std::fs::create_dir(directory.as_path().join("checkout")).map_err(storage)?;
        let mut crash = Driver::new(seed.scheduler, None);
        let schedule = crash.schedule.clone();
        let ordinal = Arc::new(AtomicUsize::new(usize::MAX));
        let dispatched = Arc::new(Mutex::new(None::<ModelDispatch>));
        let original_dispatch = Arc::clone(&dispatched);
        let dispatch_schedule = schedule.clone();
        let dispatch_ordinal = Arc::clone(&ordinal);
        let observed = AtomicUsize::new(0);
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text(seed.response.clone())])
                .with_dispatch_observer(move |dispatch| {
                    let observation = (|| {
                        *original_dispatch.lock().map_err(storage)? = Some(dispatch);
                        // The turn's own request is the first dispatch; its
                        // retrospective is the second.
                        let cut = match point {
                            Cut::FirstTurnWrite => None,
                            Cut::FirstModelEvent => Some(0),
                            Cut::Retrospective => Some(1),
                        };
                        if cut == Some(observed.fetch_add(1, Ordering::SeqCst)) {
                            dispatch_ordinal.store(
                                dispatch_schedule.cut_next(DurableFault::After),
                                Ordering::SeqCst,
                            );
                        }
                        Ok(())
                    })();
                    Box::pin(async move { observation })
                }),
        );
        let Outcome::Crashed(cause) = crash
            .run(local_turn(
                directory.as_path(),
                &schedule,
                Some(point),
                Arc::clone(&ordinal),
                Arc::clone(&provider),
                &seed,
            ))
            .await?
        else {
            return Err(Error::Invalid("turn escaped its durable write cut".into()));
        };
        assert_eq!(cause.ordinal, ordinal.load(Ordering::SeqCst));
        assert_eq!(cause.fault, DurableFault::After);
        let dispatched = *dispatched.lock().map_err(storage)?;
        assert_eq!(dispatched.is_some(), !matches!(point, Cut::FirstTurnWrite));
        if matches!(point, Cut::Retrospective) {
            assert!(
                provider
                    .last_request()
                    .is_some_and(|request| is_retrospective(&request)),
                "the cut missed the retrospective"
            );
        }
        drop(schedule);
        drop(crash);
        // Recovery consumes the retained original seed after the crash, not
        // the pre-crash in-memory copy.
        let seed = ReplaySeed::read(&seed_path)?;
        // Independent providers, stores, clocks, identity source and executor
        // reopen the original files. The provider explicitly recovers the
        // previously accepted request; no old provider object crosses the cut.
        let mut recovery = Driver::new(seed.recovery_scheduler, None);
        let schedule = recovery.schedule.clone();
        // The interrupted request completes as it would have: the turn's
        // answer, or the retrospective's reply, which never replaces it.
        let continued = if matches!(point, Cut::Retrospective) {
            "none".to_owned()
        } else {
            seed.response.clone()
        };
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text(seed.response.clone()),
                ScriptedTurn::Text(seed.response.clone()),
            ])
            .with_reconciliations(dispatched.map(|_| ScriptedReconciliation {
                orphan: true,
                observed: vec![],
                continuation: Some(vec![
                    ModelEvent::Content { delta: continued },
                    ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    },
                ]),
            })),
        );
        let result = match recovery
            .run(local_turn(
                directory.as_path(),
                &schedule,
                None,
                Arc::new(AtomicUsize::new(usize::MAX)),
                Arc::clone(&provider),
                &seed,
            ))
            .await?
        {
            Outcome::Completed(result) => result,
            Outcome::Crashed(cause) => {
                return Err(Error::Invalid(format!("uncut recovery crashed: {cause}")));
            }
        };
        result?;
        assert_eq!(
            provider.reconciliations(),
            dispatched.into_iter().collect::<Vec<_>>()
        );
        let requests = provider.requests();
        let (retrospectives, work): (Vec<_>, Vec<_>) = requests
            .iter()
            .zip(provider.dispatches())
            .partition(|(request, _)| is_retrospective(request));
        assert_eq!(
            work.len(),
            1 + usize::from(dispatched.is_none()),
            "the old request was regenerated or the new prompt did not dispatch"
        );
        // Each settlement asks once; a reconciled retrospective is not asked again.
        assert_eq!(
            retrospectives.len(),
            2 - usize::from(matches!(point, Cut::Retrospective)),
            "a settlement skipped or repeated its retrospective"
        );
        if let Some(interrupted) = dispatched {
            assert_ne!(
                interrupted.operation_id, work[0].1.operation_id,
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
    crash_reopen(Cut::FirstModelEvent)
}

#[test]
fn retrospective_cut_reopens_with_the_original_answer() -> Result<()> {
    crash_reopen(Cut::Retrospective)
}
