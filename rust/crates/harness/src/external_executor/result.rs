use crate::{Error, Outcome, Result};
use serde_json::{Value, json};

/// Projects the existing canonical tool outcome into MCP/Pi's content format.
/// Unknown or unfinished outcomes never become a successful native result.
pub fn tool_result(answer: Value) -> Result<Value> {
    let value = value(answer)?;
    let mut result = json!({"content":[{"type":"text"}],"isError":false});
    result["content"][0]["text"] = Value::String(value.to_string());
    result["details"] = value;
    Ok(result)
}

pub(super) fn value(answer: Value) -> Result<Value> {
    match serde_json::from_value(answer)
        .map_err(|_| Error::Invalid("invalid harness tool outcome".into()))?
    {
        Outcome::Succeeded(value) => Ok(value),
        Outcome::<Value>::Failed { message } => Err(Error::Invalid(message)),
        Outcome::Cancelled => Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        )),
        Outcome::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
    }
}
