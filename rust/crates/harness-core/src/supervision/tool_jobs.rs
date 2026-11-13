//! Original job lifecycle facts for alarm actors, published with their journal source.
use super::{Action, Position};
use crate::{OperationId, Result, agent::AgentPath};
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};

pub(crate) const STARTED_KEY: &[u8] = b"started";
pub(crate) const CODEC: &str = "harness.tool-quiet";

pub(crate) fn path(agent: &AgentPath) -> Result<StreamPath> {
    StreamPath::new(format!("{agent}/supervision/jobs")).map_err(Into::into)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub version: u32,
    pub call: OperationId,
    pub event: Event,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Event {
    Started {
        run: OperationId,
        execution: OperationId,
        quiet_ms: u64,
        action: Action,
        at_ms: u64,
        source: Position,
    },
    Finished,
}
