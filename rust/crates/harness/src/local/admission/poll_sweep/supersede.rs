//! A new prompt after any cut settles the interrupted head and never redispatches it.
use super::*;
use std::collections::BTreeSet;

const REPLACEMENT: &str = "replace the interrupted turn";
const REPLACED: &str = "replaced";

fn effects(fixture: &Fixture) -> (Vec<crate::model::ModelDispatch>, usize) {
    (
        fixture.provider.dispatches(),
        fixture.effect.calls.lock().unwrap().len(),
    )
}

/// Returns the replacement root and whether the reopened head was unsettled.
pub(super) async fn recover(
    fixture: &Fixture,
    reopened: &Harness,
    head: Option<&Turn>,
) -> Result<(TaskId, bool)> {
    // The killed process's providers keep no receipts: every reconciliation of
    // an in-flight model or tool effect reports unknown, as real providers do.
    fixture
        .provider
        .forget(vec![ScriptedTurn::Text(REPLACED.into()); 2]);
    *fixture.effect.forgotten.lock().unwrap() = true;
    let (models, tools) = effects(fixture);
    let dispatched: BTreeSet<_> = models
        .iter()
        .map(|dispatch| dispatch.operation_id)
        .chain(head.map(|turn| turn.operation))
        .collect();
    let prior = head.map(|turn| crate::runtime::task_id_for_admission(turn.operation));
    let interrupted = match prior {
        Some(prior) => matches!(
            reopened.runtime.task_host().outcome(prior).await,
            Ok(None | Some(crate::Outcome::Indeterminate { .. }))
        ),
        None => false,
    };
    let task = match reopened
        .submit(OperationId::from_bytes([9; 16]), REPLACEMENT)
        .await?
    {
        Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) => task_id,
        other => {
            return Err(Error::Conflict(format!(
                "replacement prompt was not admitted: {}",
                match other {
                    Admission::Rejected { reason } => reason,
                    _ => "not durable".into(),
                }
            )));
        }
    };
    if let Some(prior) = prior {
        match reopened.runtime.task_host().outcome(prior).await {
            Ok(Some(crate::Outcome::Succeeded(_) | crate::Outcome::Cancelled))
            | Err(Error::NotFound(_)) => {}
            other => {
                return Err(Error::Invalid(format!(
                    "interrupted head did not settle: {other:?}"
                )));
            }
        }
    }
    assert_eq!(
        effects(fixture),
        (models.clone(), tools),
        "settling the interrupted head dispatched an effect"
    );
    match (fixture.scenario, reopened.resume(task).await) {
        // The replacement is admitted normally, then reaches the fixture's
        // bounded active-goal stop just like its uncut baseline.
        (
            Scenario::EffectAndContinuation,
            Err(Error::GoalBlocked(crate::goal::GoalBlockedReason::Declared)),
        ) => {}
        (_, Ok(output)) => assert_eq!(output.text, REPLACED),
        (_, Err(error)) => return Err(error),
    }
    if let Some(prior) = prior {
        // Resuming the settled head observes its outcome; it never runs again.
        let _ = reopened.resume(prior).await;
    }
    let (after, calls) = effects(fixture);
    assert_eq!(calls, tools, "interrupted tool effect ran again");
    assert_eq!(after[..models.len()], models[..], "model receipts changed");
    assert!(
        after.len() > models.len()
            && after[models.len()..]
                .iter()
                .all(|dispatch| !dispatched.contains(&dispatch.operation_id)),
        "interrupted turn dispatched its model again"
    );
    Ok((task, interrupted))
}
