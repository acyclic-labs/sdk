//! Host-level workspace tests that drive a real `LocalHarness` over the
//! workspace primitives.
#![cfg(not(target_arch = "wasm32"))]

pub use crate::local::{LocalFilesystem, LocalHarness, LocalHarnessConfig};
pub use crate::runtime::RuntimeScope;
pub use crate::*;
pub use acyclic_fs::access::VolumeOperation;
pub use acyclic_fs::{CancellationToken, NativeMaterializationWork, PublicationPermit, WatchBatch};
pub use acyclic_fs_work::performance::{WorkBudget, WorkCounters};
pub use acyclic_harness_core::conversation::FileRef;
pub use acyclic_harness_core::core::{AuthorityRoot, AuthorityVerifier, Scope};
pub use acyclic_harness_core::runtime::ToolContext;
pub use acyclic_harness_core::*;
pub use acyclic_harness_workspace::checkout_tools::CheckoutTools;
pub use acyclic_harness_workspace::workspace;
pub use acyclic_harness_workspace::workspace::owner::ViewOwner;
pub use acyclic_harness_workspace::workspace::*;
pub use acyclic_harness_workspace::*;
pub use std::path::{Path, PathBuf};
pub use std::sync::Arc;

mod capture;
mod checkout_git;
mod git;
mod hooks;
mod host_workspace;
mod performance;
mod publications;
mod publisher;
mod shell_job;
mod task_views;
mod task_views_concurrency;
mod volumes_publication;
