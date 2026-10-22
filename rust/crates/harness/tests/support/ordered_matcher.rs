//! Ordered observation matching over the scenario subset relation; kept apart so
//! consumers of `subset` alone do not compile it.
use super::matchers::subset;
use serde_json::Value;

/// Matches each expectation, in order, against a distinct later observation.
pub fn ordered(expected: &[Value], actual: &[Value], label: &str) -> Result<(), String> {
    if expected.is_empty() && !actual.is_empty() {
        return Err(format!(
            "{label}: expected no observations, got {}",
            actual.len()
        ));
    }
    let mut remaining = actual.iter().enumerate();
    let mut after = 0;
    for (index, expected) in expected.iter().enumerate() {
        let Some((found, _)) = remaining.find(|(_, actual)| subset(expected, actual)) else {
            return Err(format!(
                "{label}[{index}] missing after position {after}: {expected}\nactual: {}",
                Value::Array(actual.to_vec())
            ));
        };
        after = found + 1;
    }
    Ok(())
}
