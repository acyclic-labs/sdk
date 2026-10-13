//! Selected SDK filesystem controls and their wire protocol.
use crate::{
    PiTaskBinding,
    pi_fork::{derived, original_operation},
    pi_host::{PiControlHost, denied, invalid, ready},
    pi_inspection::*,
    pi_task::PiTaskExecution,
};
use acyclic_fs::{LazySnapshotRef, WorkCounters};
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::{VolumeOperation, VolumeRef},
    durable_host::TaskJournalOwner,
    filesystem::{PreparationIo, TaskCommandProgress},
    runtime::TaskContext,
    scheduler::LeaseFence,
    store::HistoryWork,
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

mod models;
use models::*;
pub(crate) use models::{PiSelectedCommand, SELECTED_DIAGNOSTIC_LABEL_BYTES};
mod cursor;
use cursor::*;
mod commands;
mod publication;
mod source;
mod writeback;

pub(crate) fn native_workspace_error(
    (source, custody): (
        acyclic_fs::WorkspaceError,
        Option<acyclic_fs::NativeAllocation>,
    ),
) -> Error {
    Error::NativeWorkspacePublication {
        source: acyclic_fs::NativeWorkspacePublicationError::Workspace(source),
        custody,
    }
}
