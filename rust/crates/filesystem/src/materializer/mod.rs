//! Journaled pathwise application and crash recovery.
use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, MemoryRecords, next_revision};
use crate::{GenerationId, OperationId};
use serde::{Deserialize, Serialize};
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use thiserror::Error;
const JOURNAL_VERSION: u32 = 3;
/// Requires an absent endpoint during native preparation.
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
mod memory;
pub use memory::*;

macro_rules! native_modules {
    ($($visibility:vis $name:ident;)*) => {$(
        mod $name;
        $visibility use $name::*;
    )*};
}
#[cfg(not(target_arch = "wasm32"))]
native_modules! {
    pub error;
    pub work;
    pub(crate) allowance;
    pub allocation;
    pub(crate) checkout_work;
    pub(crate) views;
    pub reservation;
    pub preparation;
    pub(crate) cleanup;
    pub buffers;
    pub backend;
    pub paths;
    pub witness;
    pub metadata;
}
#[cfg(not(target_arch = "wasm32"))]
mod reservation_meter;
#[cfg(not(target_arch = "wasm32"))]
mod runtime_meter;
#[cfg(not(target_arch = "wasm32"))]
type NativeFingerprint = [u8; 32];
#[cfg(not(target_arch = "wasm32"))]
type NativeWitness = (Option<NativeFingerprint>, Option<NativeFingerprint>);

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

#[cfg(test)]
mod tests;
