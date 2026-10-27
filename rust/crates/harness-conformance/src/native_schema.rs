//! Schema-only admission for capability-skipped native fixtures. Each schema is
//! included by its native driver too; validating a case performs no native I/O.
use crate::TestResult;
use serde_json::Value;

#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness/tests/support/schema/local.rs"]
mod local;

pub(super) fn validate(mut value: Value) -> TestResult {
    let driver = value
        .as_object_mut()
        .ok_or("scenario must be an object")?
        .remove("driver")
        .ok_or("native driver is missing")?;
    match driver.as_str() {
        Some("local") => {
            let case: local::LocalCase = serde_json::from_value(value)?;
            case.validate()?;
        }
        _ => return Err("unknown native scenario driver".into()),
    }
    Ok(())
}
