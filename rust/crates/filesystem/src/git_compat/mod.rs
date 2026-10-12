//! Git-shaped compatibility state over filesystem generations.

use crate::kernel::{FileKind, NamespacePath};
use crate::model::{CheckoutMode, GenerationSelector, VolumeLimits};
use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, MemoryRecords, next_revision};
use crate::storage::ByteRange;
use crate::workspace::customer_path;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Generation, GenerationId, IdempotencyKey, OperationId,
    ResolvedFile, ResolvedFileRangeReadRequest, TransactionCommit, WorkBudget, Workspace, WorkspaceError, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use thiserror::Error;
use uuid::Uuid;

pub(super) const STATE_VERSION: u32 = 1;

pub(super) const COMMIT_DOMAIN: &[u8] = b"acyclic-fs-git-compat-commit-v1\0";

pub(super) const ACTION_DOMAIN: &[u8] = b"acyclic-fs-git-compat-action-v1\0";

pub(super) const GREP_READ_CONCURRENCY: usize = 32;

pub(super) const MAXIMUM_PATCH_FILE_BYTES: u64 = 64 * 1024 * 1024;

mod tree;
pub use tree::*;
mod objects;
pub use objects::*;
mod refs;
pub use refs::*;
#[macro_use]
mod wire;
pub use wire::*;
mod commands;
pub use commands::*;
mod store;
pub use store::*;
mod errors;
pub use errors::*;
mod repository;
pub use repository::*;
mod validation;
use validation::*;
mod dispatch;
use dispatch::*;
mod graph;
use graph::*;
mod transition;
use transition::*;
mod bisect;
use bisect::*;
mod argv;
use argv::*;
mod ignore;
pub use ignore::*;
mod capture;
pub use capture::*;
mod patch;
pub use patch::*;
mod diff;
pub use diff::*;

#[cfg(test)]
mod tests;
