//! The views served by one shared native mount instance.
//!
//! A process runs one native instance per backend (one FUSE session, one
//! `ProjFS` virtualization root, one loopback NFS mount). Every task view and
//! volume projection is a namespace inside it, identified by a serial that is
//! never reused. Kernel-held identities (inodes, handles, file ids) or the
//! view's name carry the serial, and every callback resolves it through
//! [`ViewTable::enter`], the one choke point between the shared transport and
//! a view's core:
//!
//! - a callback runs only while it holds a [`ViewLease`] on a live view;
//! - a serial that is retired, or was never issued, resolves to nothing, so a
//!   late callback fails as stale and never reaches another view's core;
//! - retirement removes the view from the table first, so no new lease can be
//!   admitted, then waits for every admitted lease to drain, and only then
//!   drops the core. The wait blocks on a condition variable; nothing polls;
//! - only the [`ViewOwner`] returned by [`ViewTable::insert`] can retire a
//!   view. Its admission-close token can move to the physical owner; finishing
//!   or dropping that token uses the same drain on every exit path.
//!
//! Retirement must not run from a callback that holds a lease on the same
//! view; the harness retires views from its own threads.

include!("view_table/implementation.rs");
