//! The public receipt contract, shared by producer replay and byte fuzzing.
use acyclic_harness::{Error, agent::AgentPath, local::host_process::Receipt};
use serde_json::Value;
use std::path::PathBuf;

pub fn verify(input: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(input) else {
        return;
    };
    let replies = value
        .as_array()
        .map_or(std::slice::from_ref(&value), Vec::as_slice);
    let Some(first) = replies.first() else {
        return;
    };
    // AgentPath owns its grammar. This contract checks the receipt's use of
    // that primitive, without copying its parser or admitting a second path.
    let task = first
        .get("task")
        .and_then(|task| serde_json::from_value::<AgentPath>(task.clone()).ok());
    let accepted = Receipt::accepted(first);
    assert_eq!(
        accepted.is_ok(),
        task.is_some(),
        "receipt admission changed"
    );
    let (Ok(receipt), Some(task)) = (accepted, task) else {
        return;
    };
    for reply in replies {
        let current = reply
            .get("task")
            .and_then(|task| serde_json::from_value::<AgentPath>(task.clone()).ok());
        let view = reply.get("cwd").and_then(Value::as_str);
        let expected = (current.as_ref() == Some(&task))
            .then_some(view)
            .flatten()
            .map(PathBuf::from);
        let result = receipt.confirm(reply);
        if current.as_ref().is_some_and(|current| current != &task) {
            assert!(
                matches!(result, Err(Error::Conflict(_))),
                "foreign task passed native receipt confirmation"
            );
        } else {
            assert_eq!(
                result.ok(),
                expected,
                "native receipt view admission changed"
            );
        }
    }
}
