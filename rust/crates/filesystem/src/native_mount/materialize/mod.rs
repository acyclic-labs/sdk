//! Explicit native materialization of one authenticated checkout.

#[cfg(any(target_os = "macos", windows))]
use crate::ObjectId;
#[cfg(test)]
use crate::demand::NativeDemandReadMeter;
use crate::demand::{DemandError, SourceDemandReadWork};
use crate::heap_future::in_heap;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, NameEncoding, NamespacePath,
};
use crate::native_host::HostRoot;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken, Checkout, FileId,
    OperationFailure, OperationReceipt, PinnedReader, ResolvedFileRangeReadRequest, WorkBudget,
    WorkCounters, WorkError,
};
#[cfg(test)]
use bytes::Bytes;
#[cfg(unix)]
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

macro_rules! read_source {
    ($reader:expr, $receipt:expr, $remaining:ident, $value:ident,
     $native:expr => $accept_native:expr, $legacy:expr => $accept_legacy:expr $(,)?) => {
        if $reader
            .validate_native_work($receipt.custody.work())
            .is_ok()
        {
            let $value = $native.await.map_err(MaterializeError::from)?;
            $accept_native
        } else {
            let $remaining = $receipt.run(|work| work.remaining())?;
            let result = $legacy
                .await
                .map_err(|failure| $receipt.engine_failure(failure))?;
            $receipt.run(|work| work.merge(result.work))?;
            let $value = result.value;
            $accept_legacy
        }
    };
}

mod work;
use work::*;
mod model;
pub use model::*;
mod api;
pub use api::*;
mod restore;
use restore::*;
mod entries;
use entries::*;
mod file;
use file::*;
mod metadata;
use metadata::*;
pub(crate) use work::NativeMaterializationActionPhase;
#[cfg(all(test, any(target_os = "macos", windows)))]
#[path = "tests/clone_tests.rs"]
mod clone_tests;
#[cfg(test)]
#[path = "tests/restore_recovery_tests.rs"]
mod restore_recovery_tests;
#[cfg(all(test, windows))]
#[path = "tests/windows_name_tests.rs"]
mod windows_name_tests;
