//! Passive selected filesystem pages; execution and source authority are never adopted.

use acyclic_fs::demand::DemandSource as _;
use acyclic_fs::{LazySnapshotRef, NativeAllocation, NativeMaterializationWork};
use acyclic_harness::filesystem::PreparationIo;
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::store::HistoryWork;
use acyclic_harness::{
    Error, Result, TaskId,
    conversation::{MAX_PRIVATE_DIRECTORY_PAGE, VolumeRef, validate_content_path},
    durable_host::TaskJournalOwner,
    filesystem::{FilesystemTaskRuntime, TaskSourceInspection},
    runtime::TaskContext,
};
use acyclic_stream::LocalStream;
use serde::{Deserialize, Serialize};

type Inspection =
    TaskSourceInspection<LocalStream, crate::LocalAuthorityBackend, crate::LocalObjectBackend>;

fn selected_unauthorized(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason,
    }
}

fn selected_invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiDifferenceKind {
    Added,
    Removed,
    Modified,
    KindChanged,
}

#[derive(Serialize)]
pub(crate) struct PiDifference {
    pub(crate) name: String,
    pub(crate) kind: PiDifferenceKind,
}

mod ancestry;
pub(crate) use ancestry::*;

mod root;
pub(crate) use root::*;

mod generations;
pub(crate) use generations::*;

mod writeback;
pub(crate) use writeback::*;

mod source;
pub(crate) use source::*;

mod import;
pub(crate) use import::*;

mod diff;
pub(crate) use diff::*;
