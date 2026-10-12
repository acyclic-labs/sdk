//! Filesystem immutable content over canonical logical Objects v1.

use crate::AsyncObjectStore;

use crate::performance::{WorkCounters, WorkError};

use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError, ObjectWrite,
    object_digest,
};

use acyclic_objects::v1::{Error, Object, ObjectsProvider, wire};

use acyclic_stream::OwnedBytes;

use std::sync::Arc;

use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};

use std::fmt::Write as _;

mod adapter;
mod common;
mod model;
mod read;
mod requests;
mod write;

use common::is_precondition;
#[cfg(not(target_arch = "wasm32"))]
use common::native_completed_read_bytes;
#[cfg(not(target_arch = "wasm32"))]
use common::native_error_code;
#[cfg(not(target_arch = "wasm32"))]
use common::native_objects_error;
use common::read_header_bytes;
use common::read_retention;
use common::service_read_error;
pub use model::LogicalObjectStore;
use requests::GetRequest;
use requests::PutRequest;
#[cfg(not(target_arch = "wasm32"))]
use requests::native_request;
use requests::put_request;
use requests::put_request_bytes;
use requests::read_request;
use requests::release_read_custody;

#[cfg(test)]
mod tests;
