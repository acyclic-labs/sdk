//! Bounded crash-recoverable local Stream provider.

use crate::StreamProviderExt as _;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};

#[cfg(test)]
use crate::OwnedBytes as Bytes;
use crate::query_sync::{RwLock, SyncMutex as Mutex, watch};
use acyclic_native_runtime::OwnershipAnchor;
#[cfg(not(loom))]
use acyclic_native_runtime::spawn_persistence as spawn_blocking;
#[cfg(all(not(loom), feature = "test-support"))]
use acyclic_native_runtime::testing::spawn;
use async_trait::async_trait;
use futures::{StreamExt as _, stream};
#[cfg(loom)]
use query_model::{spawn, spawn_blocking};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
#[cfg(all(not(loom), not(feature = "test-support")))]
use tokio::spawn;
use tokio::sync::mpsc;

#[cfg(loom)]
pub(crate) mod parking_model;
#[cfg(test)]
#[cfg(not(loom))]
mod snapshot_ready;
#[cfg(test)]
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::journal::{Command, decode_command, replay};
use crate::obs;
use crate::{
    AppendOutcome, AppendRequest, ChildrenPage, ChildrenPageRequest, CommitOutcome, CommitRequest,
    CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    MAX_COMMAND_BYTES, MemoryLimits, MemoryStream, ReadRequest, RecordStream, StreamError,
    StreamPath, StreamProvider, SystemUnixMillisClock, UnixMillisClock,
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
const INCARNATION_FILE: &str = "stream.incarnation";
const INCARNATION_TEMPORARY: &str = "stream.incarnation.tmp";
const FRAME_CHECKSUM_BYTES: usize = 32;
const REPLAY_PIPELINE_COMMANDS: usize = 32;

#[cfg(loom)]
pub(crate) mod compaction_model;
#[cfg(loom)]
pub(crate) mod follow_model;
#[cfg(loom)]
pub(crate) mod query_model;
#[cfg(loom)]
pub(crate) mod receipt_model;
#[cfg(loom)]
pub(crate) mod replay_model;

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
#[cfg(test)]
static JOURNAL_FLUSHED_BLOCKER: Mutex<Option<JournalPersistBlocker>> = Mutex::new(None);
// Lock order: journal -> test hook; release the hook before a device-gate wait.
#[cfg(test)]
static JOURNAL_COMPACT_BLOCKER: Mutex<Option<JournalPersistBlocker>> = Mutex::new(None);

#[cfg(test)]
mod tests;

include!("storage.rs");

include!("local_durability.rs");

include!("local_stream_limits.rs");

include!("local_stream_error.rs");

include!("local_stream_type.rs");

include!("local_inner.rs");

include!("pinned_commit_clock.rs");

include!("recovered.rs");

include!("local_stream.rs");

include!("persistence_cut.rs");

mod port;

include!("provider.rs");

include!("read_observation.rs");

include!("follow_state.rs");
include!("follow.rs");
include!("follow_progress.rs");

include!("journal_type.rs");

include!("prepared_frame.rs");
include!("native_mutations.rs");
include!("prepared_commit.rs");

include!("recovery_validation.rs");

include!("journal.rs");

include!("storage_functions.rs");

include!("journal_frame.rs");

include!("sync.rs");

include!("frames.rs");

include!("snapshot.rs");

include!("frame_checksum.rs");

#[cfg(all(feature = "test-support", not(loom)))]
mod publication_barrier;
