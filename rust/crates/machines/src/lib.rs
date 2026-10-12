#![doc = include_str!("../README.md")]
#![allow(
    missing_docs,
    reason = "field-level wire semantics are canonical in proto/machines/v1/machines.proto"
)]

use async_trait::async_trait;
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Generated revision-one public transport.
#[doc(hidden)]
pub mod wire {
    #![allow(missing_docs, reason = "generated from the documented public schema")]
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::allow_attributes_without_reason,
        reason = "generated protobuf bindings"
    )]
    include!("generated/acyclic.machines.v1.rs");
}

/// Canonical public descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-machines-v1.bin");
/// Current public protocol major.
pub const PROTOCOL_MAJOR: u32 = 1;
/// Current public protocol minor.
pub const PROTOCOL_MINOR: u32 = 1;
/// Maximum machines returned in one page.
pub const MAX_PAGE_SIZE: u32 = 256;
/// Largest bearer credential, in bytes, that an SDK client accepts.
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;
/// Maximum children admitted by one fork request.
pub const MAX_FORK_CHILDREN: u32 = 1_024;
/// Maximum events returned in one page.
pub const MAX_EVENT_PAGE_SIZE: u32 = 1_024;
/// Default automatic idle suspension delay.
pub const DEFAULT_IDLE_SUSPEND: Duration = Duration::from_secs(15);

mod contract;
pub use contract::*;
mod provider;
pub use provider::*;
mod simulation;
pub use simulation::*;
