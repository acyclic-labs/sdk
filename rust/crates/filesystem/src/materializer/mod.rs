//! Journaled pathwise application and crash recovery.

use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, next_revision};
use crate::{GenerationId, OperationId};
use serde::{Deserialize, Serialize};
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use thiserror::Error;

const JOURNAL_VERSION: u32 = 3;

/// Image tag requiring an absent native preparation endpoint.
#[cfg(not(target_arch = "wasm32"))]
pub const NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX: &[u8] = b"acyclic.native.prepare-absent/v1\0";

mod intent;
pub use intent::*;

mod types;
pub use types::*;

mod protocol;
pub use protocol::*;

mod inspection;
pub use inspection::*;

#[cfg(not(target_arch = "wasm32"))]
mod backend;
#[cfg(not(target_arch = "wasm32"))]
mod directories;
#[cfg(not(target_arch = "wasm32"))]
pub use backend::*;
#[cfg(not(target_arch = "wasm32"))]
use directories::*;

#[cfg(not(target_arch = "wasm32"))]
mod error;
#[cfg(not(target_arch = "wasm32"))]
pub use error::*;

#[cfg(not(target_arch = "wasm32"))]
mod allocation;
#[cfg(not(target_arch = "wasm32"))]
pub use allocation::*;

#[cfg(not(target_arch = "wasm32"))]
mod allowance;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use allowance::*;

#[cfg(not(target_arch = "wasm32"))]
mod work;
#[cfg(not(target_arch = "wasm32"))]
pub use work::*;
#[cfg(not(target_arch = "wasm32"))]
mod views;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use views::*;

#[cfg(not(target_arch = "wasm32"))]
mod runtime_meter;

#[cfg(not(target_arch = "wasm32"))]
mod reservation;
#[cfg(not(target_arch = "wasm32"))]
pub use reservation::*;
#[cfg(not(target_arch = "wasm32"))]
mod cleanup;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use cleanup::*;

#[cfg(not(target_arch = "wasm32"))]
mod buffers;
#[cfg(not(target_arch = "wasm32"))]
pub use buffers::*;
#[cfg(not(target_arch = "wasm32"))]
mod builtins;
#[cfg(not(target_arch = "wasm32"))]
mod checkout_work;
#[cfg(not(target_arch = "wasm32"))]
mod preparation;
#[cfg(not(target_arch = "wasm32"))]
pub use preparation::*;

#[cfg(not(target_arch = "wasm32"))]
mod paths;
#[cfg(not(target_arch = "wasm32"))]
pub use paths::*;

#[cfg(not(target_arch = "wasm32"))]
mod witness;
#[cfg(not(target_arch = "wasm32"))]
use witness::*;

#[cfg(not(target_arch = "wasm32"))]
mod metadata;
#[cfg(not(target_arch = "wasm32"))]
use metadata::*;

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
mod publication;
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub use publication::*;
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
mod planning;
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
use planning::*;
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
mod preflight;
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
use preflight::*;

mod memory;
pub use memory::*;

#[cfg(test)]
mod tests;
