use super::{TestResult, expected_outcome::ExpectedOutcome, ordered_matcher::ordered};
use acyclic_harness::OperationId;
use serde_json::Value;

pub(crate) fn check_frames(
    expected: &[Value],
    frames: &[Value],
    operation: OperationId,
    outcome: ExpectedOutcome,
) -> TestResult {
    let identity = operation;
    let operation = serde_json::to_value(operation)?;
    let mut terminal = None;
    for frame in frames
        .iter()
        .filter(|frame| frame["kind"] == "completed" && frame["operation_id"] == operation)
    {
        if terminal.replace(frame).is_some() {
            return Err("local task has duplicate root terminal frames".into());
        }
    }
    use serde::Deserialize as _;
    let terminal = terminal.ok_or("local task has no root terminal frame")?;
    let event = acyclic_harness::scheduler::SchedulerEvent::deserialize(terminal)?;
    match (outcome, event) {
        (
            ExpectedOutcome::Succeeded,
            acyclic_harness::scheduler::SchedulerEvent::Completed {
                outcome: acyclic_harness::Outcome::Succeeded(_),
                ..
            },
        )
        | (
            ExpectedOutcome::Failed,
            acyclic_harness::scheduler::SchedulerEvent::Completed {
                outcome: acyclic_harness::Outcome::Failed { .. },
                ..
            },
        ) => {}
        (
            ExpectedOutcome::Indeterminate,
            acyclic_harness::scheduler::SchedulerEvent::Completed {
                outcome: acyclic_harness::Outcome::Indeterminate { operation_id, .. },
                ..
            },
        ) if operation_id == identity => {}
        _ => return Err("local task wire outcome differs".into()),
    }
    ordered(expected, frames, "scheduler wire frames")?;
    Ok(())
}
