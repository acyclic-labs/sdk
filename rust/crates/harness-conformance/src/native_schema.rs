//! Schema-only admission for capability-skipped native fixtures. Each schema is
//! included by its native driver too; validating a case performs no native I/O.
use crate::TestResult;
use serde_json::Value;

#[path = "../../harness/tests/support/schema/agents.rs"]
mod agents;
#[path = "../../harness/tests/support/schema/composer_entry.rs"]
mod composer_entry;
#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness/tests/e2e/goals.rs"]
mod goals;
#[path = "../../harness/tests/support/schema/local.rs"]
mod local;
#[path = "../../harness/tests/support/schema/notifications.rs"]
mod notifications;
#[path = "../../harness/tests/support/schema/plan_query.rs"]
mod plan_query;
#[path = "../../harness/tests/support/schema/plans.rs"]
mod plans;
#[path = "../../harness/tests/support/schema/subagents.rs"]
mod subagents;
#[path = "../../harness/tests/support/schema/swarm.rs"]
mod swarm;
#[path = "../../harness/tests/support/schema/task_query.rs"]
mod task_query;

pub(super) fn validate(mut value: Value) -> TestResult {
    let driver = value
        .as_object_mut()
        .ok_or("scenario must be an object")?
        .remove("driver")
        .ok_or("native driver is missing")?;
    match driver.as_str() {
        Some("agents") => {
            let case: agents::Case = serde_json::from_value(value)?;
            case.validate()?;
        }
        Some("goal-local") => {
            goals::GoalCase::parse(value)?;
        }
        Some("composer_entry") => {
            composer_entry::ComposerEntry::parse(value)?;
        }
        Some("local") => {
            let case: local::LocalCase = serde_json::from_value(value)?;
            case.validate()?;
        }
        Some("notifications") => {
            notifications::Notifications::parse(value)?;
        }
        Some("plans") => {
            plans::Plans::parse(value)?;
        }
        Some("plan-query") => {
            plan_query::PlanQuery::parse(value)?;
        }
        Some("task_query") => {
            let _: task_query::TaskQueryCase = serde_json::from_value(value)?;
        }
        Some("subagents") => {
            let case: subagents::SubagentCase = serde_json::from_value(value)?;
            case.validate()?;
        }
        Some("swarm") => {
            let case: swarm::SwarmCase = serde_json::from_value(value)?;
            case.validate()?;
        }
        _ => return Err("unknown native scenario driver".into()),
    }
    Ok(())
}
