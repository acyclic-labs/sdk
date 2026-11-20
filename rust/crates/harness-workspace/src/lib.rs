//! The selected checkout, its volumes and the tools bound to it.

#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::cognitive_complexity,
        reason = "unit tests run ordered scenarios over fixtures they built"
    )
)]

// The core's items keep their `crate::` paths here, as in the facade.
use crate::{
    conversation::{VolumeOperation, VolumeRef},
    core::{AuthorityRoot, AuthorityVerifier, Scope},
    filesystem::FilesystemContentPublisher,
    runtime::RuntimeScope,
    tool::ToolRegistry,
};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness_core::filesystem::FilesystemHost;
use acyclic_harness_core::*;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub mod checkout_tools;
pub mod host_workspace;
pub mod tool_metadata;
/// The selected checkout, its volumes and the views bound to it.
pub mod workspace;

pub use workspace::{LocalWorkspace, WorkspaceMounting};

/// The session volume shared by a local root and its descendants.
pub const SHARED_VOLUME: &str = "shared";

/// Local durable filesystem host.
pub type LocalFilesystem<A = LocalAuthorityBackend, O = LocalObjectBackend> = FilesystemHost<A, O>;

/// Maps a displayable failure into the storage error the host reports.
pub fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
