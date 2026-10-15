use super::{TestResult, composer::Composer, execution, notifications::Notifications, scenario};
use serde_json::Value;

pub(super) async fn run(mut value: Value) -> TestResult {
    let driver = value
        .as_object_mut()
        .ok_or("scenario must be an object")?
        .remove("driver");
    match driver.as_ref().and_then(Value::as_str) {
        None if driver.is_none() => execution::run(scenario::Scenario::from_value(value)?).await,
        Some("executor") => execution::run(scenario::Scenario::from_value(value)?).await,
        Some("composer") => Composer::parse(value)?.run().await,
        Some("notifications") => Notifications::parse(value)?.run(),
        Some("plans") => super::plans::Plans::parse(value)?.run().await,
        _ => Err("unknown scenario driver".into()),
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
