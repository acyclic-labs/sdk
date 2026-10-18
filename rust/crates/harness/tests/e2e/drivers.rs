use super::task_query_case::TaskQueryCase;
use super::{TestResult, composer::Composer, execution, notifications::Notifications, scenario};
use serde_json::Value;

// Raw local execution is visible only inside this guarded dispatcher.
#[path = "local_execution.rs"]
mod local_execution;
pub(super) use local_execution::{check_frames, check_results, model_results};

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

impl Driver for super::local_case::LocalCase {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        local_execution::run(case).await
    }
}

impl Driver for super::swarm_case::SwarmCase {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        execute_swarm(case, super::swarm_wire::Fault::None).await
    }
}

async fn execute_swarm(
    case: super::swarm_case::SwarmCase,
    fault: super::swarm_wire::Fault,
) -> TestResult {
    super::swarm::run_with_fault(case, fault, false)
        .await
        .map(drop)
}

impl Driver for super::subagents::SubagentCase {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        super::subagents::run(case).await
    }
}

impl Driver for super::provider_case::ProviderCase {
    type Case = Self;
    async fn execute(case: Self) -> TestResult {
        super::providers::run(case).await
    }
}

async fn run_execution(case: scenario::Scenario) -> TestResult {
    TaskDriver::execute(case).await
}

pub(super) enum Case {
    Recipe(Value),
    Execution(Box<scenario::Scenario>),
    SwarmFault(Box<super::swarm_case::SwarmCase>, super::swarm_wire::Fault),
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
        Case::SwarmFault(case, fault) => Box::pin(execute_swarm(*case, fault)).await,
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
        #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
        Some("agents") => Box::pin(super::agents::run(value)).await,
        None if driver.is_none() => {
            Box::pin(run_execution(scenario::Scenario::from_value(value)?)).await
        }
        Some("executor") => Box::pin(run_execution(scenario::Scenario::from_value(value)?)).await,
        Some("composer") => Box::pin(Composer::execute(Composer::parse(value)?)).await,
        Some("notifications") => {
            Box::pin(Notifications::execute(Notifications::parse(value)?)).await
        }
        Some("task_query") => {
            Box::pin(TaskQueryCase::execute(serde_json::from_value(value)?)).await
        }
        Some("subagents") => {
            Box::pin(super::subagents::SubagentCase::execute(
                serde_json::from_value(value)?,
            ))
            .await
        }
        Some("provider") => {
            Box::pin(super::provider_case::ProviderCase::execute(
                serde_json::from_value(value)?,
            ))
            .await
        }
        Some("local") => {
            Box::pin(super::local_case::LocalCase::execute(
                serde_json::from_value(value)?,
            ))
            .await
        }
        Some("swarm") => {
            Box::pin(super::swarm_case::SwarmCase::execute(
                serde_json::from_value(value)?,
            ))
            .await
        }
        Some("plans") => {
            Box::pin(super::plans::Plans::execute(super::plans::Plans::parse(
                value,
            )?))
            .await
        }
        Some("plan-query") => {
            Box::pin(super::plan_query::PlanQuery::execute(
                super::plan_query::PlanQuery::parse(value)?,
            ))
            .await
        }
        _ => Err("unknown scenario driver".into()),
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
