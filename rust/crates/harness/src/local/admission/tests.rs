use super::*;
use crate::runtime::TaskStateProvider;
use crate::{
    context::CompactionPolicy,
    executor::TurnOutput,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};

mod bootstrap;
mod replay;

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
