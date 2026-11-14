//! Authenticated native mount and raw workspace access.
use super::PreparationIo;
use crate::{
    Error, Result,
    conversation::{VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, NativeAllocation, NativeMaterializationWork,
};
use std::sync::Arc;
mod access;
mod mount_authority;
pub use mount_authority::NativeMountAuthority;
#[cfg(not(target_arch = "wasm32"))]
mod local_owner;
pub(in crate::filesystem) use access::owner_mount_options;
use access::{NativeAccessAuthority, authenticated_mount_options};
pub use access::{
    WorkspaceAccess, require_unrestricted_volume_access, require_workspace_subtree_read,
};
#[cfg(not(target_arch = "wasm32"))]
pub use local_owner::{
    local_owner_mount_options, local_owner_workspace, local_owner_workspace_access,
};
