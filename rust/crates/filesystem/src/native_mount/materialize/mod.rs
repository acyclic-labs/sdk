//! Explicit native materialization of one authenticated checkout.

#[cfg(any(target_os = "macos", windows))]
use crate::ObjectId;
#[cfg(test)]
use crate::demand::NativeDemandReadMeter;
use crate::demand::{DemandError, SourceDemandReadWork};
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
#[cfg(any(windows, test))]
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

macro_rules! read_source {
    ($reader:expr, $receipt:ident, $remaining:ident, $value:ident,
     $native:expr => $accept_native:expr, $legacy:expr => $accept_legacy:expr $(,)?) => {
        if $reader
            .validate_native_work($receipt.custody.work())
            .is_ok()
        {
            // Boxed on the receipt: source reads are the deepest step frames.
            let $value = in_receipt_frame(&mut *$receipt, |$receipt| $native)
                .await?
                .map_err(MaterializeError::from)?;
            $accept_native
        } else {
            // The frame is admitted on the custody before the read's budget is
            // taken, and its claim is held until the read's work is merged, so
            // the read can never spend the bytes of the frame it runs in.
            let $value = in_receipt_frame(&mut *$receipt, |$receipt| {
                let legacy = SourceDemandReadWork::remaining(&mut $receipt.custody.work().clone())
                    .map(|$remaining| $legacy);
                async move {
                    let result = legacy
                        .map_err(MaterializeError::from)?
                        .await
                        .map_err(|failure| $receipt.engine_failure(failure))?;
                    $receipt
                        .custody
                        .work()
                        .clone()
                        .merge(result.work)
                        .map_err(MaterializeError::from)?;
                    Ok::<_, MaterializeError>(result.value)
                }
            })
            .await??;
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
mod selection;
#[cfg(fuzzing)]
pub use selection::verify_native_staging_selection;
mod restore;
use restore::*;
mod entries;
use entries::*;
mod file;
use file::*;
mod metadata;
use metadata::*;
pub(crate) use work::NativeMaterializationActionPhase;
#[cfg(test)]
#[cfg(any(target_os = "macos", windows))]
#[path = "tests/clone_tests.rs"]
mod clone_tests;
#[cfg(test)]
#[path = "tests/policy_metadata.rs"]
mod policy_metadata_tests;
#[cfg(test)]
#[path = "tests/restore_recovery_tests.rs"]
mod restore_recovery_tests;
#[cfg(test)]
#[cfg(windows)]
#[path = "tests/windows_name_tests.rs"]
mod windows_name_tests;
