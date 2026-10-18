use super::*;
use crate::{
    context::CompactionPolicy,
    executor::TurnOutput,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};

mod bootstrap;

// Test-only observation of the admission gate boundary. Recording is active
// only inside `observe`, which a test wraps around one synchronous poll, so
// other cases sharing the thread cannot interleave with it.
pub(super) mod probe {
    use std::cell::{Cell, RefCell};
    thread_local! {
        static LOG: RefCell<Option<Vec<&'static str>>> = const { RefCell::new(None) };
        static UNGATED: Cell<bool> = const { Cell::new(false) };
    }

    pub(crate) fn note(event: &'static str) {
        LOG.with_borrow_mut(|log| {
            if let Some(log) = log {
                log.push(event);
            }
        });
    }

    /// Entry to the admission gate; true only for the ungated mutant, which
    /// then runs its action without the gate.
    pub(crate) fn arrive(held: bool) -> bool {
        if UNGATED.get() {
            note("gate skipped");
            return true;
        }
        if held {
            note("gate wait");
        }
        false
    }

    pub(super) fn observe<T>(ungated: bool, poll: impl FnOnce() -> T) -> (T, Vec<&'static str>) {
        LOG.set(Some(Vec::new()));
        UNGATED.set(ungated);
        let result = poll();
        UNGATED.set(false);
        (result, LOG.take().unwrap_or_default())
    }
}

// Interrupt the actual production publication future at each admission await
// boundary: HEAD acknowledged before admission, and admission committed before
// its acknowledgement reaches the caller. A push signal selects the cut; no
// timeout, sleep, background task, or timing assumption controls cancellation.
async fn cancel_at_admission_await(harness: &LocalHarness, after_admission: bool) -> Result<Turn> {
    let operation = OperationId::new();
    let inputs = harness.stage_turn(operation, "original input").await?;
    let (signal, ready) = tokio::sync::oneshot::channel();
    let mut submission = Box::pin(serialized(&harness.admission, async || {
        let previous = harness.current_turn().await?.0.map(|turn| turn.operation);
        harness
            .publish_turn_with(
                Turn {
                    operation,
                    root: operation,
                    automatic_turns: 0,
                    goal_revision: None,
                    receive: inputs.0,
                    model: inputs.1,
                },
                previous,
                async move |turn| {
                    if after_admission {
                        admitted_task(harness.admit_retained(turn).await?)?;
                    }
                    signal
                        .send(turn.clone())
                        .map_err(|_| Error::Conflict("cancellation observer dropped".into()))?;
                    std::future::pending::<Result<Admission<RuntimeTask<TurnOutput>>>>().await
                },
            )
            .await
    }));
    let retained = tokio::select! {
        _ = &mut submission => return Err(Error::Conflict("submission did not suspend".into())),
        retained = ready => retained.map_err(|_| Error::Conflict("publication did not reach its await boundary".into()))?,
    };
    drop(submission);
    Ok(retained)
}

async fn unadmitted_request_head_does_not_block_a_new_run(
    harness: &LocalHarness,
    provider: &ScriptedModelProvider,
) -> Result<()> {
    let retained = cancel_at_admission_await(harness, false).await?;
    let task = crate::runtime::task_id_for_admission(retained.operation);
    // Negative control: the former unconditional outcome query errors here.
    assert!(matches!(
        harness.runtime.task_host().outcome(task).await,
        Err(Error::NotFound(_))
    ));
    assert!(harness.head_is_terminal(Some(&retained)).await?);
    assert_eq!(harness.run("replacement input").await?.text, "done");
    assert_request(provider, 1, "replacement input");
    assert!(
        harness
            .runtime
            .harness()
            .reconcile_admission(retained.operation)
            .await?
            .is_none(),
        "a replaced orphan request was accidentally admitted"
    );
    Ok(())
}

fn assert_request(provider: &ScriptedModelProvider, count: usize, input: &str) {
    let requests = provider.requests();
    assert_eq!(requests.len(), count, "missing or duplicated model effect");
    assert!(requests[count - 1].messages.iter().any(|message| {
        message.role == crate::model::ModelRole::User
            && matches!(&message.content, crate::model::ModelContent::Text(text) if text == input)
    }), "the model did not receive this case's retained input");
}

#[tokio::test]
async fn cancellation_at_each_head_admission_await_resumes_the_retained_input_once() -> Result<()> {
    let started = std::time::Instant::now();
    let bootstrap = bootstrap::Bootstrap::new().await?;
    // Both orders restore identical immutable bytes into distinct roots; every
    // case has its own script, owner/provider caches, and model effect ledger.
    // Overlap independent I/O on the existing runtime; each order stays serial.
    async fn run_order(
        bootstrap: &bootstrap::Bootstrap,
        order: [Option<bool>; 3],
    ) -> Result<Vec<Option<bool>>> {
        let mut cases = Vec::new();
        for after_admission in order {
            let (root, provider) = bootstrap.restore()?;
            let directory = root.directory.path().join("root");
            let harness =
                LocalHarness::open(bootstrap.config(&directory, provider.clone())?).await?;
            assert_eq!(harness.signed.agent(), bootstrap.agent);
            assert!(harness.current_turn().await?.0.is_none());
            assert!(provider.requests().is_empty());
            let Some(after_admission) = after_admission else {
                unadmitted_request_head_does_not_block_a_new_run(&harness, &provider).await?;
                cases.push(None);
                continue;
            };
            let retained = cancel_at_admission_await(&harness, after_admission).await?;
            let task = crate::runtime::task_id_for_admission(retained.operation);
            let definition = harness
                .runtime
                .harness()
                .task::<serde_json::Value, TurnOutput>("acyclic.stock_turn@1")?;
            let old_attach = harness.runtime.harness().attach(task, &definition).await;
            assert_eq!(
                old_attach.is_ok(),
                after_admission,
                "negative control did not reach the unadmitted-head failure"
            );
            drop(old_attach);
            drop(harness);
            let harness =
                LocalHarness::open(bootstrap.config(&directory, provider.clone())?).await?;
            assert_eq!(harness.signed.agent(), bootstrap.agent);
            assert_eq!(harness.resume(task).await?.text, "done");
            assert_eq!(harness.resume(task).await?.text, "done");
            assert_eq!(harness.current_turn().await?.0, Some(retained));
            assert_request(&provider, 1, "original input");
            assert_eq!(provider.remaining(), 0);
            cases.push(Some(after_admission));
        }
        Ok(cases)
    }
    let (forward, reverse) = tokio::try_join!(
        Box::pin(run_order(&bootstrap, [None, Some(false), Some(true)])),
        Box::pin(run_order(&bootstrap, [Some(true), Some(false), None])),
    )?;
    assert_eq!(forward, [None, Some(false), Some(true)]);
    assert_eq!(reverse, [Some(true), Some(false), None]);
    eprintln!("admission recovery suite: {:?}", started.elapsed());
    Ok(())
}

// Both admission races drive real LocalHarness entry points over restored,
// disjoint roots. Holder A composes the production head read and publication
// under a gate and parks at the race's cut; entry point B then runs. The
// healthy arm parks A under the harness admission gate. The mutant parks A
// under a separate gate, which is exactly a missing admission lock: B then
// proceeds past A's cut and the shared detector must report it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Race {
    // A published its request head and is suspended before admission; B is a
    // fresh submission. Only the gate keeps B from replacing the unadmitted
    // head (which counts as terminal) before A admits it.
    SubmitAfterPublication,
    // A observed an orphaned unadmitted head and is suspended before replacing
    // it; B recovers that orphan. Only the gate keeps both from admitting.
    RecoveryBeforeReplacement,
}

// The one oracle for both arms: exactly one of the competing operations holds
// a durable admission. The expected count derives from the race, not from B.
async fn single_admission(
    harness: &LocalHarness,
    operations: [OperationId; 2],
) -> Result<std::result::Result<(), &'static str>> {
    let mut admitted = 0;
    for operation in operations {
        let reconciled = harness
            .runtime
            .harness()
            .reconcile_admission(operation)
            .await?;
        admitted += usize::from(reconciled.is_some());
    }
    Ok(match admitted {
        1 => Ok(()),
        0 => Err("no admission"),
        _ => Err("double admission"),
    })
}

async fn admission_race(
    bootstrap: &bootstrap::Bootstrap,
    race: Race,
    serialized_holder: bool,
    gated_competitor: bool,
) -> Result<std::result::Result<(), &'static str>> {
    let (root, provider) = bootstrap.restore()?;
    let directory = root.directory.path().join("root");
    let owned = LocalHarness::open(bootstrap.config(&directory, provider.clone())?).await?;
    let harness = &owned;
    let orphan = match race {
        Race::SubmitAfterPublication => None,
        Race::RecoveryBeforeReplacement => Some(cancel_at_admission_await(harness, false).await?),
    };
    let operation = OperationId::new();
    let submitted = OperationId::new();
    let inputs = harness.stage_turn(operation, "holder input").await?;
    let separate = StateArc::new(Mutex::new(()));
    let gate = if serialized_holder {
        &harness.admission
    } else {
        &separate
    };
    let (reached, at_cut) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let holder = serialized(gate, async || {
        let previous = harness.current_turn().await?.0;
        assert_eq!(previous.as_ref(), orphan.as_ref());
        if let Some(previous) = &previous {
            // Exactly submit_turn's replacement predicate for an orphan head.
            assert!(can_admit(
                Some(previous.operation),
                None,
                harness.head_is_terminal(Some(previous)).await?
            ));
        }
        let mut cut = Some((reached, released));
        if race == Race::RecoveryBeforeReplacement {
            let (reached, released) = cut.take().expect("one cut");
            reached.send(()).ok();
            released.await.ok();
        }
        harness
            .publish_turn_with(
                Turn {
                    operation,
                    root: operation,
                    automatic_turns: 0,
                    goal_revision: None,
                    receive: inputs.0,
                    model: inputs.1,
                },
                previous.map(|turn| turn.operation),
                async move |turn| {
                    if let Some((reached, released)) = cut {
                        reached.send(()).ok();
                        released.await.ok();
                    }
                    harness.admit_retained(turn).await
                },
            )
            .await
    });
    let mut holder = std::pin::pin!(holder);
    tokio::select! {
        biased;
        _ = at_cut => {}
        _ = &mut holder => return Err(Error::Conflict("holder finished before its cut".into())),
    }
    let competitor = async {
        match orphan.as_ref() {
            None => harness
                .submit_turn(submitted, "competing input")
                .await
                .map(|admission| matches!(admission, Admission::Rejected { .. })),
            Some(orphan) => match harness
                .recover_unadmitted_turn(crate::runtime::task_id_for_admission(orphan.operation))
                .await
            {
                Ok(_) => Ok(false),
                Err(Error::NotFound(_)) => Ok(true),
                Err(error) => Err(error),
            },
        }
    };
    let mut competitor = std::pin::pin!(competitor);
    use futures::FutureExt as _;
    // B enters only through the admission gate. While A holds it, B's first
    // poll must park on that very gate with no head read and no model request;
    // otherwise (the separate-gate mutant) B runs to completion first.
    let early = if harness.admission.try_lock().is_err() {
        let requests = provider.requests().len();
        let (pending, seen) = probe::observe(!gated_competitor, || {
            competitor.as_mut().now_or_never().is_none()
        });
        if !pending || seen != ["gate wait"] {
            // B bypassed the gate and may be suspended holding internal state that A
            // needs, so awaiting either could deadlock: cancel both by dropping.
            return Ok(Err("competitor bypassed the admission gate"));
        }
        assert_eq!(
            provider.requests().len(),
            requests,
            "B reached the model provider while A held the gate"
        );
        None
    } else {
        Some(competitor.as_mut().await?)
    };
    release.send(()).ok();
    admitted_task(holder.await?)?;
    let declined = match early {
        Some(declined) => declined,
        None => competitor.await?,
    };
    let competing = orphan.as_ref().map_or(submitted, |orphan| orphan.operation);
    let verdict = single_admission(harness, [operation, competing]).await?;
    assert_eq!(
        verdict.is_ok(),
        declined,
        "B outcome disagrees with admissions"
    );
    Ok(verdict)
}

#[tokio::test]
async fn admission_entry_points_cannot_both_admit_across_the_gate() -> Result<()> {
    use futures::FutureExt as _;
    let bootstrap = bootstrap::Bootstrap::new().await?;
    let bypassed = Err("competitor bypassed the admission gate");
    // (race, holder uses the production gate, competitor uses it, verdict)
    let arms = [
        (Race::SubmitAfterPublication, true, true, Ok(())),
        (
            Race::SubmitAfterPublication,
            false,
            true,
            Err("double admission"),
        ),
        (Race::SubmitAfterPublication, true, false, bypassed),
        (Race::RecoveryBeforeReplacement, true, true, Ok(())),
        (
            Race::RecoveryBeforeReplacement,
            false,
            true,
            Err("double admission"),
        ),
        (Race::RecoveryBeforeReplacement, true, false, bypassed),
    ];
    let verdicts = futures::future::try_join_all(arms.iter().map(|&(race, holder, gated, _)| {
        admission_race(&bootstrap, race, holder, gated).boxed_local()
    }))
    .await?;
    for (verdict, (race, holder, gated, expected)) in verdicts.into_iter().zip(arms) {
        assert_eq!(
            verdict, expected,
            "{race:?} holder={holder} competitor={gated}"
        );
    }
    Ok(())
}
