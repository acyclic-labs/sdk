//! Native file and owned-process primitives shared by SDK host consumers.

use bytes::Bytes;
use std::cell::Cell;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, Weak, mpsc};
use std::task::{Context, Poll, Waker};

pub mod obs;
mod process_tree;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::{read_at as read_at_impl, write_all_at as write_all_at_impl};
#[cfg(target_os = "linux")]
use linux::{read_at as read_at_impl, write_all_at as write_all_at_impl};
#[cfg(all(unix, not(target_os = "linux")))]
use apple::{read_batch as read_batch_impl, write_all_batch_owned as write_all_batch_impl};

pub use process_tree::{CaptureInput, ProcessCaptureFailure, ProcessInput, ProcessTree};
#[cfg(unix)]
pub use process_tree::PreparedClosedProcess;

mod file;
pub(crate) use file::{NativeFilePayload, is_uncertain_io_error, with_file_scope, with_file_admission, write_bounded_in_place, validate_write_batch, read_bounded_in_place, validate_prepared_read_range, read_prepared_in_place};
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) use file::{FileIdentity, file_identity};
#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) use file::{write_all_batch_owned};
#[cfg(all(test, any(windows, target_os = "linux", target_vendor = "apple")))]
pub(crate) use file::{in_file_scope};
pub use file::{NativeFile, NativeFileMeter, Durability, OwnedRead, OwnedWrite, NativeReadResult, AsyncRangeReader, sync_file, sync_data, sync_parent, read_at, write_all_at, read_batch_async, set_len};
#[cfg(windows)]
pub use file::device_control_in_place;
#[cfg(unix)]
pub use file::read_declared_source_bytes;
mod ownership;
pub use ownership::{OwnershipAnchor, OwnershipReleaseBarrier};
mod recovery;
pub use recovery::{LogTail, recover_log_tail, is_exclusive_lock_contention};
mod namespace;
pub use namespace::{RenameMode, publish_new, durable_rename};
mod service;
pub use service::{SERVICE_READY_ARGUMENT, ServiceReadiness, ServiceReadySignal, take_service_ready_signal, spawn_service_process, spawn_process_tree, spawn_process_tree_with_meter_custody, process_output, process_status};
mod meter;
pub use meter::{NativeOperationKind, NativeReadMeter, NativeOperationReservation, NativeByteReservation, ReadAllocation, ReadAllocationReservation};
mod blocking;
pub(crate) use blocking::{inline_blocking_allowed, task_completion, BlockingWaitWake, measured_completion_meter, wait_measured_completion};
pub use blocking::{InlineBlocking, run_blocking_io};
mod completion;
pub(crate) use completion::{OperationFence, CompletionState, TaskTerminal, FileCompletion};
#[cfg(any(windows, target_os = "linux"))]
pub(crate) use completion::{submit_file_io};
pub use completion::{NativeCompletion};
mod workers;
pub(crate) use workers::{NativeJob, NativeUnitOperation, NativeWorkers, NATIVE_WORKERS, BLOCKING_WORKERS, workers, CapacityWaiter, allocate_capacity_waiter, poll_submission, remove_capacity_waiter, wake_capacity_waiter};
#[cfg(test)]
pub(crate) use workers::{poll_submission_with, AdmissionState, notify_capacity_released};
pub use workers::{initialize_native_io_services};

mod health;
use health::FileHealth;
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
use health::{FILE_HEALTH, FileHealthRegistry, poison_file_health, shared_file_health};

#[cfg(test)]
mod tests;
