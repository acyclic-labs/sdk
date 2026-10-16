use super::task_query_case::TaskQueryCase;
use super::{TestResult, composer::Composer, execution, notifications::Notifications, scenario};
use serde_json::Value;

trait Driver {
    type Case;
    fn execute(case: Self::Case) -> impl Future<Output = TestResult> + Send;
}

struct JournalDriver;

// Change this selection when production consumes and publishes task-volume files.
type TaskDriver = JournalDriver;

impl Driver for JournalDriver {
    type Case = scenario::Scenario;
    async fn execute(case: Self::Case) -> TestResult {
        execution::run(case).await
    }
}

impl Driver for Composer {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        case.run().await
    }
}

impl Driver for Notifications {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        case.run()
    }
}

impl Driver for super::plans::Plans {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        case.run().await
    }
}

impl Driver for super::plan_query::PlanQuery {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        case.run().await
    }
}

impl Driver for TaskQueryCase {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        super::task_queries::run(case).await
    }
}

impl Driver for super::diff_case::DiffCase {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        super::diffs::run(case)
    }
}

pub(super) async fn run_execution(case: scenario::Scenario) -> TestResult {
    TaskDriver::execute(case).await
}

pub(super) async fn run(mut value: Value) -> TestResult {
    let driver = value
        .as_object_mut()
        .ok_or("scenario must be an object")?
        .remove("driver");
    match driver.as_ref().and_then(Value::as_str) {
        None if driver.is_none() => run_execution(scenario::Scenario::from_value(value)?).await,
        Some("executor") => run_execution(scenario::Scenario::from_value(value)?).await,
        Some("composer") => Composer::execute(Composer::parse(value)?).await,
        Some("notifications") => Notifications::execute(Notifications::parse(value)?).await,
        Some("task_query") => TaskQueryCase::execute(serde_json::from_value(value)?).await,
        Some("diff") => super::diff_case::DiffCase::execute(serde_json::from_value(value)?).await,
        Some("plans") => super::plans::Plans::execute(super::plans::Plans::parse(value)?).await,
        Some("plan-query") => {
            super::plan_query::PlanQuery::execute(super::plan_query::PlanQuery::parse(value)?).await
        }
        _ => Err("unknown scenario driver".into()),
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
