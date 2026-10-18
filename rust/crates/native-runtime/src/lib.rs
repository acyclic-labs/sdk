//! Native file and owned-process primitives shared by SDK host consumers.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

use bytes::Bytes;
use std::cell::Cell;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::mpsc;
use std::sync::{Arc, Condvar, LazyLock, Mutex, Weak};
use std::task::{Context, Poll, Waker};
mod sync;
use sync::Mutex as CompletionMutex;

#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
mod held_sync;
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub use held_sync::sync_descriptor_once;
pub mod obs;
mod process_tree;
#[cfg(windows)]
mod windows;
#[cfg(all(unix, not(target_os = "linux")))]
use apple::{read_batch as read_batch_impl, write_all_batch_owned as write_all_batch_impl};
#[cfg(target_os = "linux")]
use linux::{read_at as read_at_impl, write_all_at as write_all_at_impl};
#[cfg(target_vendor = "apple")]
#[path = "apple_terminal.rs"]
pub mod terminal;
#[cfg(windows)]
pub use windows::terminal;
#[cfg(windows)]
use windows::{read_at as read_at_impl, write_all_at as write_all_at_impl};

#[cfg(unix)]
pub use process_tree::PreparedClosedProcess;
pub use process_tree::{
    CaptureInput, ProcessCaptureFailure, ProcessCustody, ProcessInput, ProcessOutput, ProcessTree,
};

mod file;
#[cfg(windows)]
pub use file::device_control_in_place;
#[cfg(unix)]
pub use file::read_declared_source_bytes;
#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) use file::write_all_batch_owned;
pub use file::{
    AsyncRangeReader, Durability, NativeFile, NativeFileMeter, OwnedRead, OwnedWrite,
    flush_file_all, flush_file_data, read_at, read_batch_async, rename_file, set_len, sync_data,
    sync_file, sync_parent, truncate_file, write_all_at, write_file, write_file_all,
    write_file_with,
};
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) use file::{FileIdentity, file_identity};
pub(crate) use file::{
    NativeFilePayload, is_uncertain_io_error, validate_prepared_read_range, validate_write_batch,
    with_file_admission, with_file_scope, write_bounded_in_place,
};
mod ownership;
pub use ownership::{OwnershipAnchor, OwnershipReleaseBarrier};
mod file_lock;
pub use file_lock::{FileLock, FileLockMode};
mod recovery;
pub use recovery::{LogTail, is_exclusive_lock_contention, recover_log_tail};
mod directory;
pub use directory::remove_child_directory;
mod namespace;
pub use namespace::{RenameMode, durable_rename, publish_new};
mod service;
mod shell;
pub use service::{
    SERVICE_READY_ARGUMENT, ServiceReadiness, ServiceReadySignal, ServiceStartOutcome,
    process_output, process_status, spawn_service_process, take_service_ready_signal,
};
pub use shell::shell_command;
mod meter;
pub use meter::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    ReadAllocation, ReadAllocationReservation,
};
mod blocking;
mod task;
#[cfg(feature = "test-support")]
pub mod testing;
pub use blocking::{InlineBlocking, run_blocking_io};
pub(crate) use blocking::{
    PreparedWake, inline_blocking_allowed, measured_completion_meter, task_completion,
    wait_measured_completion,
};
pub use task::{AbortHandle, TaskHandle, spawn, spawn_blocking};
mod completion;
pub use completion::NativeCompletion;
#[cfg(any(windows, target_os = "linux"))]
pub(crate) use completion::submit_file_io;
pub(crate) use completion::{CompletionState, FileCompletion, OperationFence, TaskTerminal};
mod workers;
pub use workers::initialize_native_io_services;
#[cfg(test)]
pub(crate) use workers::{AdmissionState, notify_capacity_released, poll_submission_with};
pub(crate) use workers::{
    BLOCKING_WORKERS, CapacityWaiter, NATIVE_WORKERS, NativeJob, NativeWorkers, capacity_waiter,
    poll_submission, prepay_capacity_waiter, remove_capacity_waiter, wake_capacity_waiter, workers,
};

mod health;
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
use health::{FILE_HEALTH, FileHealthRegistry, poison_file_health, shared_file_health};
use health::{FileHealth, HealthOwner};

#[cfg(test)]
mod tests;
