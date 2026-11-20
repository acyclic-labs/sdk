use super::{
    TestResult,
    diff_case::{DiffCase, Input},
};
use acyclic_harness::client::FileDiff;
use serde_json::{Value, json};

pub(super) fn capture(case: &DiffCase) -> TestResult<Value> {
    let before = case.before.as_ref().map(Input::bytes);
    let after = case.after.as_ref().map(Input::bytes);
    match FileDiff::between(before, after, case.render, case.work) {
        Ok(diff) => {
            // Retain the former goldens' exact render-boundary rejection check.
            if let Some(diff) = &diff
                && !diff.patch.is_empty()
                && FileDiff::between(before, after, diff.patch.len() - 1, case.work).is_ok()
            {
                return Err("diff accepted a render bound below its patch size".into());
            }
            Ok(serde_json::to_value(diff)?)
        }
        Err(error) => Ok(json!({"error":error.to_string()})),
    }
}

pub(super) fn run(case: DiffCase) -> TestResult {
    let actual = capture(&case)?;
    if actual != case.expect {
        return Err(format!("diff: expected {}, got {actual}", case.expect).into());
    }
    Ok(())
}
