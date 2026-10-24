//! An admitted command retains its task independently of how execution ends.
use crate::{Error, Result, TaskId};
use serde_json::Value;

pub(crate) struct CommandExecution {
    task: Option<TaskId>,
    result: Result<Value>,
}

impl CommandExecution {
    pub(crate) fn admitted(task: TaskId, result: Result<Value>) -> Self {
        Self {
            task: Some(task),
            result,
        }
    }

    pub(crate) fn into_parts(self) -> (Option<TaskId>, Result<Value>) {
        (self.task, self.result)
    }
}

impl From<Result<Value>> for CommandExecution {
    fn from(result: Result<Value>) -> Self {
        if let Err(Error::TaskFailed { task, .. }) = &result {
            return Self::admitted(*task, result);
        }
        Self { task: None, result }
    }
}
