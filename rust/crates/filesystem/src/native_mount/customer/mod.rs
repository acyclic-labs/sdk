//! Customer-facing native mount composition over the canonical checkout.

use super::adapter::HostCaptureScope;
use super::{
    CheckoutMountSource, LazyMountSource, MountFilesystem, MountPath, NativeMountError,
    NativeMountRequest, NativeMountSession, SharedCheckout,
};
use crate::demand::{DemandSource, SourceNode, SourceNodeKind};
use crate::kernel::FileKind;
use crate::lazy_workspace::LazyTreapWork;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::native_capture::NativeViewBaseline;
use crate::workspace::{Workspace, WorkspaceError};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, GenerationId, IdempotencyKey, LazyLookup, LazyWorkspace,
    LazyWorkspaceError, LazyWorkspaceStore, MountId, MountSourceError, NativeAllocation,
    NativeMaterializationWork, NativeRootIdentity, PublicationPermit, VolumeId,
};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;

mod options;
mod mount;
mod lazy_mount;
mod working_set;
use working_set::*;
mod generation;
mod publication;
mod lifecycle;
use lifecycle::*;
mod lazy_workspace;
pub use lazy_mount::LazyMount;
pub use lifecycle::MountLifecycleError;
pub use lifecycle::NativeMountDrainProof;
pub use mount::Mount;
pub use options::{MountOptions, MountPublication};
pub use publication::NativeMountPublicationFailure;
pub use working_set::NativeWorkingSet;

#[cfg(test)]
mod mounted_tests;
#[cfg(test)]
mod tests;
