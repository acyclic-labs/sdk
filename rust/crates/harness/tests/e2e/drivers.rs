use super::{TestResult, composer::Composer, execution, scenario};
use serde_json::Value;

// Raw local execution is visible only inside this guarded dispatcher.
#[path = "local_execution.rs"]
mod local_execution;
pub(super) use local_execution::{check_results, model_results};

trait Driver {
    type Case;
    fn execute(case: Self::Case) -> impl Future<Output = TestResult>;
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

impl Driver for super::local_case::LocalCase {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        local_execution::run(case).await
    }
}

async fn run_execution(case: scenario::Scenario) -> TestResult {
    TaskDriver::execute(case).await
}

pub(super) enum Case {
    Recipe(Value),
    Execution(Box<scenario::Scenario>),
}
impl From<Value> for Case {
    fn from(value: Value) -> Self {
        Self::Recipe(value)
    }
}
impl From<scenario::Scenario> for Case {
    fn from(value: scenario::Scenario) -> Self {
        Self::Execution(Box::new(value))
    }
}

pub(super) async fn run(case: impl Into<Case>) -> TestResult {
    // The shared corpus already owns a guard; direct tests enter the same path.
    Box::pin(super::stall::guarded(
        std::path::Path::new("driver"),
        dispatch(case.into()),
    ))
    .await
    .map_err(std::io::Error::other)?
}

async fn dispatch(case: Case) -> TestResult {
    match case {
        Case::Execution(case) => Box::pin(run_execution(*case)).await,
        Case::Recipe(value) => dispatch_recipe(value).await,
    }
}

async fn dispatch_recipe(mut value: Value) -> TestResult {
    let driver = value
        .as_object_mut()
        .ok_or("scenario must be an object")?
        .remove("driver");
    // Keep each selected driver frame out of the shared dispatch frame. Otherwise
    // every cheap case retains the largest native driver state across each await.
    match driver.as_ref().and_then(Value::as_str) {
        None if driver.is_none() => {
            Box::pin(run_execution(scenario::Scenario::from_value(value)?)).await
        }
        Some("executor") => Box::pin(run_execution(scenario::Scenario::from_value(value)?)).await,
        Some("composer") => Box::pin(Composer::execute(Composer::parse(value)?)).await,
        Some("local") => {
            Box::pin(super::local_case::LocalCase::execute(
                serde_json::from_value(value)?,
            ))
            .await
        }
        _ => Err("unknown scenario driver".into()),
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
