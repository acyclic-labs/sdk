//! Filesystem immutable content over canonical logical Objects v1.

use crate::AsyncObjectStore;

use crate::cancellation::CancellationToken;

use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};

use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectResult,
    ObjectStoreError, ObjectWrite, object_digest,
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

pub use model::LogicalObjectStore;
use requests::PutRequest;
use requests::GetRequest;
use requests::put_request_bytes;
use requests::put_request;
#[cfg(not(target_arch = "wasm32"))]
use requests::native_request;
use requests::release_read_custody;
use requests::read_request;
use common::service_read_error;
#[cfg(not(target_arch = "wasm32"))]
use common::native_objects_error;
#[cfg(not(target_arch = "wasm32"))]
use common::native_completed_read_bytes;
use common::read_retention;
#[cfg(not(target_arch = "wasm32"))]
use common::native_error_code;
use common::is_precondition;
use common::read_header_bytes;
use read::LogicalReadResults;
use read::finish_reads;
use write::record_write_completion;
use write::release_write_custody;
use write::put_info_bytes;
use write::LogicalPutResults;

#[cfg(test)]
mod tests;
