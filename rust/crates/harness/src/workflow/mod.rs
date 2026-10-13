//! Pinned resumable machines with atomic checkpoints and command outboxes.

mod copies;
mod host;
mod io;
mod journal;
mod machine;
mod memory;
mod records;
#[cfg(test)]
mod tests;

pub(crate) use copies::workflow_copy_identity;
pub use host::DurableWorkflowHost;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use io::finish_native_io;
pub(crate) use io::workflow_owned_bytes;
pub use journal::WorkflowJournal;
pub use machine::{MachineIdentity, MachineRegistry, ResumableMachine};
pub use memory::MemoryWorkflowJournal;
pub use records::{
    MachineCheckpoint, MachineStatus, MachineTransition, WorkflowAdmission, WorkflowCommand,
    WorkflowRecord,
};
pub(crate) use records::{validate_workflow_commit, validate_workflow_next};

use crate::executor::{PreparationIo, stock_boundary, with_history_io};
use crate::wire::ErrorCode::{Conflict, Invalid, Storage, Unsupported};
use crate::{Error, IdempotencyKey, NativeCustody, OperationId, Result, conversation::FileRef};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use io::{next_revision, validate_key};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub(crate) const WORKFLOW_REPLAY_PAGE_RECORDS: u32 = 64;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn unsupported(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: Unsupported,
        reason,
    }
}
