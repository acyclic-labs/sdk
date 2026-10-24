//! A cancellation has one immutable task-owned identity across clients and execution.
use super::{Component, FileKey, Result, TaskFile};
use crate::{
    TaskId,
    command_request::{CommandRequest, CommandResolution},
};

/// Exact request identity for cancellation of an admitted task.
pub fn cancellation_key(task: TaskId) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string())?,
        file: TaskFile::Request(Component::new(format!("cancel-{task}"))?),
    })
}

/// Validate both the task-owned path and the targeted cancellation payload.
pub fn is_cancellation(task: TaskId, key: &FileKey, bytes: &[u8]) -> bool {
    if !cancellation_key(task).is_ok_and(|expected| expected == *key) {
        return false;
    }
    let request = serde_json::from_slice::<CommandResolution>(bytes)
        .ok()
        .and_then(|value| match value {
            CommandResolution::Submit { request } => Some(request),
            _ => None,
        })
        .or_else(|| serde_json::from_slice::<CommandRequest>(bytes).ok());
    matches!(request, Some(CommandRequest::Cancel { task: target }) if target == task)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_requires_exact_owner_path_and_payload()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let task = TaskId::new();
        let other = TaskId::new();
        let key = cancellation_key(task)?;
        let request = CommandRequest::Cancel { task };
        for bytes in [
            serde_json::to_vec(&request)?,
            serde_json::to_vec(&CommandResolution::Submit { request })?,
        ] {
            assert!(is_cancellation(task, &key, &bytes));
            assert!(!is_cancellation(other, &key, &bytes));
            assert!(!is_cancellation(task, &cancellation_key(other)?, &bytes));
        }
        let mismatched = serde_json::to_vec(&CommandRequest::Cancel { task: other })?;
        assert!(!is_cancellation(task, &key, &mismatched));
        assert!(!is_cancellation(task, &key, b"{}"));
        Ok(())
    }
}
