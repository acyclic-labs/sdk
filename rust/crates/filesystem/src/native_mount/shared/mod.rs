//! The one OS-neutral core behind every shared native instance.
//!
//! A process serves every task view and volume projection from one native
//! instance per root: one FUSE session on Linux, one `ProjFS` virtualization
//! root on Windows, one loopback NFS mount on macOS. Everything those
//! instances have in common lives here, written once:
//!
//! - [`view_table`]: view serials, never reused, and the lease every
//!   callback holds; retirement unlists a view and drains its leases, so a
//!   late callback is stale by construction;
//! - [`views`]: the names under which views are listed and looked up, and
//!   refusal of new views once an instance stops;
//! - [`instances`]: the per-root registry that starts an instance for its
//!   first view and tears it down after its last;
//! - [`root`]: the process's root, held under an owner lock for the
//!   process's lifetime, and the startup reaper for dead processes' roots;
//! - [`outcome`]: what a failed callback reports, as one classification
//!   each OS spells in its own codes, and whether it stops the mount;
//! - [`callback_gate`]: the in-flight callback count, whose drain blocks
//!   without polling and finishes however many callbacks keep arriving;
//! - [`metering`]: which meter a callback charges, the guard that
//!   authorizes it, and the scope that keeps an invocation's meter active.
//!
//! A per-OS adapter holds only what its OS API forces on it: callback
//! signatures, how a callback names its view (an inode's high bits, a path
//! prefix), how a root is enumerated and deleted, and the spelling of a
//! [`outcome::CallbackFailure`].

// FUSE sessions drain through their kernel connection instead.
#[cfg(any(target_os = "macos", windows, test))]
pub(super) mod callback_gate;
// Only the shared instances of these OSes are built on the core yet; the
// macOS NFS adapter plugs into the same modules.
#[cfg(any(target_os = "linux", windows, loom))]
pub(super) mod instances;
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub(super) mod metering;
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub(super) mod outcome;
mod root;
#[cfg(any(target_os = "linux", windows, loom))]
pub(super) mod view_table;
#[cfg(any(target_os = "linux", windows, loom))]
pub(super) mod views;

#[cfg(windows)]
pub(super) use root::claim_issued_destination;
pub(crate) use root::shared_view_destination;
pub use root::{shared_view_namespace_contains, shared_view_root, shared_view_root_in};
