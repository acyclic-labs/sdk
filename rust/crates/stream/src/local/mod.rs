//! Bounded crash-recoverable local Stream provider.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(test)]
use crate::OwnedBytes;
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    OwnershipAnchor, ReadAllocation,
};
use async_trait::async_trait;
use fs2::FileExt as _;
use futures::{StreamExt as _, stream};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock, mpsc, watch};
#[cfg(test)]
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::journal::{Command, decode_command, replay};
use crate::obs;
use crate::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest, ChildrenRequest,
    CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, MAX_COMMAND_BYTES, MemoryLimits, MemoryStream, NativeProviderFuture,
    NativeRecordStream, NativeStreamOutput, NativeStreamResult, ReadRequest, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, SystemUnixMillisClock, UnixMillisClock,
};

const HEADER_MAGIC: &[u8; 24] = b"ACYCLIC-STREAM-LOCAL-V1\0";
/// The magic, the limits, then the journal's epoch.
const HEADER_BYTES: usize = HEADER_MAGIC.len() + LIMITS_BYTES + 8;
const LIMITS_BYTES: usize = 8 * 8;
const SNAPSHOT_MAGIC: &[u8; 24] = b"ACYCLIC-STREAM-SNAP-V01\0";
/// The magic, limits and journal epoch; state and checksum follow.
const SNAPSHOT_HEADER_BYTES: usize = SNAPSHOT_MAGIC.len() + LIMITS_BYTES + 8;
/// A frame's sampled Unix commit time, before its command.
const FRAME_TIME_BYTES: usize = 8;
const LOCK_FILE: &str = "stream.lock";
const JOURNAL_FILE: &str = "stream.journal";
const SNAPSHOT_FILE: &str = "stream.snapshot";
const SNAPSHOT_TEMPORARY: &str = "stream.snapshot.tmp";
const FRAME_CHECKSUM_BYTES: usize = 32;
const FRAME_DOMAIN: &[u8] = b"acyclic-stream-local-frame-v1\0";
const SNAPSHOT_DOMAIN: &[u8] = b"acyclic-stream-local-snapshot-v1\0";
const REPLAY_PIPELINE_COMMANDS: usize = 32;

use crate::memory::{MemoryFailure, MemoryResult, native_result};

mod frame;
mod io;
mod journal;
mod native;
mod provider;
mod publication;
mod snapshot;
mod startup;
#[cfg(test)]
mod tests;
use frame::*;
use io::*;
use journal::*;
use native::*;
use publication::*;
use snapshot::*;
#[cfg(test)]
static JOURNAL_OPEN_SUBMITTED: Mutex<Option<(PathBuf, tokio::sync::oneshot::Sender<()>)>> =
    Mutex::new(None);
#[cfg(test)]
type JournalPersistBlocker = (
    usize,
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);
#[cfg(test)]
static JOURNAL_PERSIST_BLOCKER: Mutex<Option<JournalPersistBlocker>> = Mutex::new(None);

/// How the journal is made durable before a mutation becomes observable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Flushes the device cache before acknowledging publication.
    #[default]
    FullFlush,
    /// Orders publication behind earlier writes without flushing the device cache.
    Barrier,
}

tokio::task_local! {
    /// Whether mutations on this task leave their frames unflushed.
    static DEFERRED: bool;
}

/// Defers device flushes until [`LocalStream::flush`] or a nondeferred mutation.
pub async fn deferring_durability<F: Future>(future: F) -> F::Output {
    DEFERRED.scope(true, future).await
}

fn deferred() -> bool {
    DEFERRED.try_with(|deferred| *deferred).unwrap_or(false)
}

/// Explicit local retention and recovery bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalStreamLimits {
    /// Canonical in-memory semantic-state limits.
    pub memory: MemoryLimits,
    /// Maximum durable commands replayed at startup.
    pub journal_operations: u64,
    /// Maximum journal bytes, including framing.
    pub journal_bytes: u64,
    /// Journal synchronization policy. Not recorded in the durable header.
    pub durability: LocalDurability,
}

impl Default for LocalStreamLimits {
    fn default() -> Self {
        Self {
            memory: MemoryLimits::default(),
            journal_operations: 1_000_000,
            journal_bytes: 1024 * 1024 * 1024,
            durability: LocalDurability::FullFlush,
        }
    }
}

/// Local provider initialization or durable-publication failure.
#[derive(Debug, Error)]
pub enum LocalStreamError {
    /// Filesystem operation failed.
    #[error("local Stream I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Another process owns the local provider root.
    #[error("local Stream root is already open")]
    AlreadyOpen,
    /// Stored bytes or configuration do not match the canonical local format.
    #[error("local Stream journal is corrupt or incompatible")]
    Corrupt,
    /// Configured bounds are zero or cannot represent the canonical format.
    #[error("local Stream limits are invalid")]
    InvalidLimits,
    /// Durable history cannot be reconstructed through the canonical state machine.
    #[error("local Stream replay failed: {0}")]
    Replay(StreamError),
    /// Native blocking execution could not complete.
    #[error("local Stream executor is unavailable")]
    Executor,
    /// An accepted first write completed only a prefix.
    #[error("local Stream prepared frame is incomplete")]
    IncompleteFrame,
}

impl LocalStreamError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::AlreadyOpen => "already_open",
            Self::Corrupt => "corrupt",
            Self::InvalidLimits => "invalid_limits",
            Self::Replay(error) => error.code(),
            Self::Executor => "executor",
            Self::IncompleteFrame => "incomplete_frame",
        }
    }
}

/// Exclusive durable provider backed by snapshots and a checksummed journal.
#[derive(Clone)]
pub struct LocalStream {
    inner: Arc<LocalInner>,
}

struct LocalInner {
    provider: MemoryStream,
    journal: Mutex<Journal>,
    sync: File,
    _ownership_anchor: Option<OwnershipAnchor>,
    /// Held exclusively while a mutation applies and writes its frame, and shared while a read observes state; never across a device flush.
    visibility: Arc<RwLock<()>>,
    /// The journal's `synced` sequence, published under the journal lock so that callers can wait for their frame, and those before it, to flush.
    synced: watch::Sender<u64>,
    changed: watch::Sender<u64>,
    poisoned: AtomicBool,
}

impl LocalInner {
    fn check_available(&self) -> Result<(), StreamError> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
    }

    fn poison(&self) -> StreamError {
        self.poisoned.store(true, Ordering::Release);
        // Wake every caller waiting for a flush, so it sees the poison.
        self.synced.send_modify(|_| {});
        StreamError::Unavailable
    }

    /// Records that every frame through `target` is durable.
    fn publish_synced(&self, journal: &mut Journal, target: u64) {
        journal.synced = journal.synced.max(target);
        let synced = journal.synced;
        self.synced.send_modify(|published| *published = synced);
    }

    /// Flushes the journal through `target`, the latest frame written when this flush began, on a blocking thread.
    fn flush_through(
        &self,
        target: u64,
        durability: LocalDurability,
        mut admission: Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
    ) -> MemoryResult<()> {
        #[cfg(test)]
        {
            let inner_identity = self as *const Self as usize;
            if let Ok(mut hook) = JOURNAL_PERSIST_BLOCKER.lock()
                && hook
                    .as_ref()
                    .is_some_and(|(expected_inner, _, _)| *expected_inner == inner_identity)
                && let Some((_, started, release)) = hook.take()
            {
                let _ = started.send(());
                let _ = release.recv();
            }
        }
        let (flushed, observed) = match &mut admission {
            Some(admission) => admission.attempt(|| sync_file_data(&self.sync, durability)),
            None => (sync_file_data(&self.sync, durability), Ok(())),
        };
        let Ok(mut journal) = self.journal.lock() else {
            self.poison();
            return Err(StreamError::Unavailable.into());
        };
        journal.syncing = false;
        if flushed.is_ok() {
            self.publish_synced(&mut journal, target);
        } else {
            drop(journal);
            self.poison();
        }
        flushed?;
        observed?;
        Ok(())
    }
}

fn flush(
    plan: NativeStreamOutput<(
        Arc<LocalInner>,
        LocalDurability,
        Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
    )>,
    target: u64,
) -> MemoryResult<()> {
    let (inner, durability, admission) = plan.value;
    inner.flush_through(target, durability, admission)
}

fn ordinary_failure(error: MemoryFailure) -> StreamError {
    match error {
        MemoryFailure::Provider(error) => error,
        MemoryFailure::Observer(_) => StreamError::Unavailable,
    }
}

fn publication_error(error: LocalStreamError) -> MemoryFailure {
    match error {
        LocalStreamError::Io(error) => MemoryFailure::Observer(error),
        LocalStreamError::InvalidLimits => MemoryFailure::Provider(StreamError::Capacity),
        LocalStreamError::Replay(error) => MemoryFailure::Provider(error),
        _ => MemoryFailure::Provider(StreamError::Unavailable),
    }
}

fn finish_native<T>(
    span: &obs::OperationSpan,
    result: NativeStreamResult<T>,
) -> NativeStreamResult<T> {
    match &result {
        Ok(Ok(_)) => {
            let _ = obs::finish(span, Ok::<(), StreamError>(()));
        }
        Ok(Err(error)) => obs::failed(span, error.code()),
        Err(_) => obs::failed(span, "native_work"),
    }
    result
}

/// What recovery hands the state machine, in order.
enum Recovered {
    /// Authorizes disk repair only after every recovered semantic operation passed.
    Validate(tokio::sync::oneshot::Sender<bool>),
    /// The snapshot's state, first.
    State { encoded: Vec<u8> },
    /// Each command in the journal after it.
    Command {
        committed_at_millis: u64,
        command: Command,
    },
}
