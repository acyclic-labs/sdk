//! Bounded crash-recoverable local Stream provider.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt as _;
use futures::{StreamExt as _, stream};
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{RwLock, watch};

use crate::wire_codec::{condition_from_wire, mutation_from_wire, optional_key, required_key};
use crate::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitOutcome, CommitRequest,
    CommittedEnvelope, DeleteReceipt, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, MAX_COMMAND_BYTES, MAX_ITEMS, MemoryLimits, MemoryStream, ReadRequest,
    RecordStream, StreamError, StreamPath, StreamProvider, TrimReceipt,
};

const HEADER_MAGIC: &[u8; 24] = b"ACYCLIC-STREAM-LOCAL-V1\0";
const HEADER_BYTES: usize = HEADER_MAGIC.len() + 8 * 8;
const FRAME_CHECKSUM_BYTES: usize = 32;

/// How the journal is made durable before a mutation becomes observable.
///
/// A per-open policy, not part of the on-disk contract: a journal written under one policy
/// reopens under any other.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Every publication waits for the storage device to flush its cache (`F_FULLFSYNC` on
    /// Apple platforms, `fsync`/`fdatasync` elsewhere), so acknowledged frames survive power
    /// loss.
    #[default]
    FullFlush,
    /// Every publication is ordered behind earlier writes without waiting for the device
    /// cache (`F_BARRIERFSYNC` on Apple platforms, `fdatasync` elsewhere). Acknowledged
    /// frames survive process crashes; a power loss before the device drains its cache can
    /// lose the newest ones, which startup recovery discards as a torn tail.
    Barrier,
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
}

/// Exclusive-process durable local provider backed by a checksummed command journal.
///
/// The journal is synchronized before a mutation becomes observable. Startup replays every
/// complete frame through the same bounded [`MemoryStream`] state machine used by conformance.
/// A torn final frame is removed; corruption in a complete frame fails closed.
#[derive(Clone)]
pub struct LocalStream {
    inner: Arc<LocalInner>,
}

struct LocalInner {
    provider: MemoryStream,
    journal: Arc<Mutex<Journal>>,
    visibility: RwLock<()>,
    changed: watch::Sender<u64>,
    poisoned: AtomicBool,
}

impl LocalStream {
    /// Opens or creates one exclusive durable provider below `root`.
    pub async fn open(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
    ) -> Result<Self, LocalStreamError> {
        validate_limits(limits)?;
        let root = root.as_ref().to_path_buf();
        let loaded = tokio::task::spawn_blocking(move || Journal::open(&root, limits))
            .await
            .map_err(|_| LocalStreamError::Executor)??;
        let provider = MemoryStream::new(limits.memory);
        for command in loaded.commands {
            replay(&provider, command)
                .await
                .map_err(LocalStreamError::Replay)?;
        }
        let (changed, _) = watch::channel(0_u64);
        Ok(Self {
            inner: Arc::new(LocalInner {
                provider,
                journal: Arc::new(Mutex::new(loaded.journal)),
                visibility: RwLock::new(()),
                changed,
                poisoned: AtomicBool::new(false),
            }),
        })
    }

    fn check_available(&self) -> Result<(), StreamError> {
        if self.inner.poisoned.load(Ordering::Acquire) {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
    }

    async fn prepare(&self, command: Command) -> Result<PreparedFrame, StreamError> {
        let journal = Arc::clone(&self.inner.journal);
        tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| LocalStreamError::Corrupt)?
                .prepare(command)
        })
        .await
        .map_err(|_| LocalStreamError::Executor)
        .and_then(|result| result)
        .map_err(|error| match error {
            LocalStreamError::InvalidLimits => StreamError::Capacity,
            _ => StreamError::Unavailable,
        })
    }

    async fn persist(&self, frame: PreparedFrame) -> Result<(), StreamError> {
        let journal = Arc::clone(&self.inner.journal);
        let result = tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| LocalStreamError::Corrupt)?
                .append(frame)
        })
        .await
        .map_err(|_| LocalStreamError::Executor)
        .and_then(|result| result);
        if result.is_err() {
            self.inner.poisoned.store(true, Ordering::Release);
            return Err(StreamError::Unavailable);
        }
        self.inner.changed.send_modify(|revision| {
            *revision = revision.saturating_add(1);
        });
        Ok(())
    }

    async fn read_visible(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.read(request).await
    }
}

#[async_trait]
impl StreamProvider for LocalStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner
            .provider
            .inspect_idempotency(idempotency_key)
            .await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.tail(path).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Append(request.clone());
        let frame = self.prepare(command).await?;
        let retain_conflict = request.idempotency_key.is_some();
        let outcome = self.inner.provider.append(request).await?;
        if matches!(outcome, AppendOutcome::Committed(_)) || retain_conflict {
            self.persist(frame).await?;
        }
        Ok(outcome)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Fork(request.clone());
        let frame = self.prepare(command).await?;
        let outcome = self.inner.provider.fork(request).await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn trim(
        &self,
        path: StreamPath,
        before: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TrimReceipt, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Trim {
            path: path.clone(),
            before,
            idempotency_key: idempotency_key.clone(),
        };
        let frame = self.prepare(command).await?;
        let outcome = self
            .inner
            .provider
            .trim(path, before, idempotency_key)
            .await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn delete(
        &self,
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    ) -> Result<DeleteReceipt, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Delete {
            path: path.clone(),
            idempotency_key: idempotency_key.clone(),
        };
        let frame = self.prepare(command).await?;
        let outcome = self.inner.provider.delete(path, idempotency_key).await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.read_visible(request).await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.tail(path.clone()).await?;
        let state = FollowState {
            provider: self.clone(),
            path,
            next: from,
            changed: self.inner.changed.subscribe(),
            done: false,
        };
        Ok(stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            loop {
                match state
                    .provider
                    .read_visible(ReadRequest {
                        path: state.path.clone(),
                        from: state.next,
                        limit: 1,
                    })
                    .await
                {
                    Ok(mut records) => match records.next().await {
                        Some(Ok(record)) => {
                            state.next = record.sequence.saturating_add(1);
                            return Some((Ok(record), state));
                        }
                        Some(Err(error)) => {
                            state.done = true;
                            return Some((Err(error), state));
                        }
                        None => {}
                    },
                    Err(error) => {
                        state.done = true;
                        return Some((Err(error), state));
                    }
                }
                if state.changed.changed().await.is_err() {
                    state.done = true;
                    return Some((Err(StreamError::Unavailable), state));
                }
            }
        })
        .boxed())
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.children(request).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Commit(request.clone());
        let frame = self.prepare(command).await?;
        let outcome = self.inner.provider.commit(request).await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn read_commit(
        &self,
        commit_id: crate::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.read_commit(commit_id).await
    }
}

struct FollowState {
    provider: LocalStream,
    path: StreamPath,
    next: u64,
    changed: watch::Receiver<u64>,
    done: bool,
}

struct LoadedJournal {
    journal: Journal,
    commands: Vec<Command>,
}

struct Journal {
    file: File,
    operations: u64,
    bytes: u64,
    limits: LocalStreamLimits,
    _root: PathBuf,
}

struct PreparedFrame {
    length: [u8; 4],
    command: Vec<u8>,
    checksum: [u8; FRAME_CHECKSUM_BYTES],
    bytes: u64,
}

impl Journal {
    fn open(root: &Path, limits: LocalStreamLimits) -> Result<LoadedJournal, LocalStreamError> {
        std::fs::create_dir_all(root)?;
        let path = root.join("stream.journal");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.try_lock_exclusive()
            .map_err(|_| LocalStreamError::AlreadyOpen)?;
        let length = file.metadata()?.len();
        if length == 0 {
            let header = encode_header(limits)?;
            file.write_all(&header)?;
            sync_file(&file, limits.durability)?;
            sync_directory(root, limits.durability)?;
        } else {
            let mut header = vec![0_u8; HEADER_BYTES];
            file.read_exact(&mut header)
                .map_err(|_| LocalStreamError::Corrupt)?;
            if header != encode_header(limits)? {
                return Err(LocalStreamError::Corrupt);
            }
        }
        let mut commands = Vec::new();
        let mut valid_length =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        let total_length = file.metadata()?.len();
        file.seek(SeekFrom::Start(valid_length))?;
        while valid_length < total_length {
            let frame_start = valid_length;
            let remaining = total_length.saturating_sub(frame_start);
            if remaining < 4 {
                truncate_torn_tail(&mut file, frame_start, limits.durability)?;
                valid_length = frame_start;
                break;
            }
            let mut length_bytes = [0_u8; 4];
            file.read_exact(&mut length_bytes)?;
            let command_length = u64::from(u32::from_le_bytes(length_bytes));
            let frame_length = 4_u64
                .checked_add(command_length)
                .and_then(|value| value.checked_add(u64::try_from(FRAME_CHECKSUM_BYTES).ok()?))
                .ok_or(LocalStreamError::Corrupt)?;
            if remaining < frame_length {
                truncate_torn_tail(&mut file, frame_start, limits.durability)?;
                valid_length = frame_start;
                break;
            }
            if command_length == 0
                || command_length
                    > u64::try_from(MAX_COMMAND_BYTES)
                        .map_err(|_| LocalStreamError::InvalidLimits)?
            {
                return Err(LocalStreamError::Corrupt);
            }
            let command_length =
                usize::try_from(command_length).map_err(|_| LocalStreamError::Corrupt)?;
            let mut encoded = vec![0_u8; command_length];
            file.read_exact(&mut encoded)?;
            let mut checksum = [0_u8; FRAME_CHECKSUM_BYTES];
            file.read_exact(&mut checksum)?;
            if frame_checksum(&length_bytes, &encoded) != checksum {
                return Err(LocalStreamError::Corrupt);
            }
            commands.push(decode_command(&encoded).map_err(|_| LocalStreamError::Corrupt)?);
            if u64::try_from(commands.len()).map_err(|_| LocalStreamError::Corrupt)?
                > limits.journal_operations
            {
                return Err(LocalStreamError::Corrupt);
            }
            valid_length = valid_length
                .checked_add(frame_length)
                .ok_or(LocalStreamError::Corrupt)?;
        }
        if valid_length > limits.journal_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        file.seek(SeekFrom::End(0))?;
        Ok(LoadedJournal {
            journal: Self {
                file,
                operations: u64::try_from(commands.len()).map_err(|_| LocalStreamError::Corrupt)?,
                bytes: valid_length,
                limits,
                _root: root.to_path_buf(),
            },
            commands,
        })
    }

    fn prepare(&self, command: Command) -> Result<PreparedFrame, LocalStreamError> {
        let encoded = encode_command(&command)?;
        let command_length =
            u32::try_from(encoded.len()).map_err(|_| LocalStreamError::InvalidLimits)?;
        let length_bytes = command_length.to_le_bytes();
        let checksum = frame_checksum(&length_bytes, &encoded);
        let frame_bytes = 4_u64
            .checked_add(u64::from(command_length))
            .and_then(|value| value.checked_add(u64::try_from(FRAME_CHECKSUM_BYTES).ok()?))
            .ok_or(LocalStreamError::InvalidLimits)?;
        if self.operations >= self.limits.journal_operations
            || self
                .bytes
                .checked_add(frame_bytes)
                .is_none_or(|value| value > self.limits.journal_bytes)
        {
            return Err(LocalStreamError::InvalidLimits);
        }
        Ok(PreparedFrame {
            length: length_bytes,
            command: encoded,
            checksum,
            bytes: frame_bytes,
        })
    }

    fn append(&mut self, frame: PreparedFrame) -> Result<(), LocalStreamError> {
        self.file.write_all(&frame.length)?;
        self.file.write_all(&frame.command)?;
        self.file.write_all(&frame.checksum)?;
        sync_file_data(&self.file, self.limits.durability)?;
        self.operations += 1;
        self.bytes += frame.bytes;
        Ok(())
    }
}

fn truncate_torn_tail(
    file: &mut File,
    valid_length: u64,
    durability: LocalDurability,
) -> Result<(), LocalStreamError> {
    file.set_len(valid_length)?;
    sync_file(file, durability)?;
    file.seek(SeekFrom::Start(valid_length))?;
    Ok(())
}

fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    match durability {
        LocalDurability::FullFlush => file.sync_all(),
        LocalDurability::Barrier => barrier_sync(file),
    }
}

fn sync_file_data(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    match durability {
        LocalDurability::FullFlush => file.sync_data(),
        LocalDurability::Barrier => barrier_sync(file),
    }
}

/// Orders every earlier write to `file` ahead of later ones without waiting for the device
/// cache. Falls back to a full flush where the filesystem cannot issue a barrier.
#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
fn barrier_sync(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: `F_BARRIERFSYNC` takes no argument and only acts on the descriptor, which
    // `file` keeps open for the duration of the call.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } == 0 {
        Ok(())
    } else {
        file.sync_all()
    }
}

#[cfg(not(target_vendor = "apple"))]
fn barrier_sync(file: &File) -> std::io::Result<()> {
    file.sync_data()
}

#[cfg(unix)]
fn sync_directory(path: &Path, durability: LocalDurability) -> Result<(), LocalStreamError> {
    sync_file(&File::open(path)?, durability)?;
    Ok(())
}

#[cfg(windows)]
fn sync_directory(_path: &Path, _durability: LocalDurability) -> Result<(), LocalStreamError> {
    // `FlushFileBuffers` on the newly created journal durably admits both its contents and
    // directory entry on supported Windows filesystems. Windows does not expose a portable
    // flush operation for directory handles.
    Ok(())
}

fn validate_limits(limits: LocalStreamLimits) -> Result<(), LocalStreamError> {
    let memory = limits.memory;
    if memory.paths == 0
        || memory.path_bytes == 0
        || memory.records == 0
        || memory.payload_bytes == 0
        || memory.commits == 0
        || memory.idempotency_results == 0
        || limits.journal_operations == 0
        || limits.journal_bytes
            < u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?
    {
        Err(LocalStreamError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn encode_header(limits: LocalStreamLimits) -> Result<Vec<u8>, LocalStreamError> {
    let mut encoded = Vec::with_capacity(HEADER_BYTES);
    encoded.extend_from_slice(HEADER_MAGIC);
    for value in [
        limits.memory.paths,
        limits.memory.path_bytes,
        limits.memory.records,
        limits.memory.payload_bytes,
        limits.memory.commits,
        limits.memory.idempotency_results,
    ] {
        encoded.extend_from_slice(
            &u64::try_from(value)
                .map_err(|_| LocalStreamError::InvalidLimits)?
                .to_le_bytes(),
        );
    }
    encoded.extend_from_slice(&limits.journal_operations.to_le_bytes());
    encoded.extend_from_slice(&limits.journal_bytes.to_le_bytes());
    Ok(encoded)
}

fn frame_checksum(length: &[u8; 4], command: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"acyclic-stream-local-frame-v1\0");
    hasher.update(length);
    hasher.update(command);
    hasher.finalize().into()
}

#[derive(Clone)]
enum Command {
    Append(AppendRequest),
    Fork(ForkRequest),
    Trim {
        path: StreamPath,
        before: u64,
        idempotency_key: IdempotencyKey,
    },
    Delete {
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    },
    Commit(CommitRequest),
}

async fn replay(provider: &MemoryStream, command: Command) -> Result<(), StreamError> {
    match command {
        Command::Append(request) => provider.append(request).await.map(|_| ()),
        Command::Fork(request) => provider.fork(request).await.map(|_| ()),
        Command::Trim {
            path,
            before,
            idempotency_key,
        } => provider
            .trim(path, before, idempotency_key)
            .await
            .map(|_| ()),
        Command::Delete {
            path,
            idempotency_key,
        } => provider.delete(path, idempotency_key).await.map(|_| ()),
        Command::Commit(request) => provider.commit(request).await.map(|_| ()),
    }
}

fn encode_command(command: &Command) -> Result<Vec<u8>, LocalStreamError> {
    let operation = match command {
        Command::Append(request) => journal_command::Operation::Append(wire_append(request)),
        Command::Fork(request) => journal_command::Operation::Fork(wire_fork(request)),
        Command::Trim {
            path,
            before,
            idempotency_key,
        } => journal_command::Operation::Trim(crate::wire::TrimRequest {
            path: path.to_string(),
            before: *before,
            idempotency_key: Some(Bytes::copy_from_slice(idempotency_key.as_bytes())),
        }),
        Command::Delete {
            path,
            idempotency_key,
        } => journal_command::Operation::Delete(crate::wire::DeleteRequest {
            path: path.to_string(),
            idempotency_key: Some(Bytes::copy_from_slice(idempotency_key.as_bytes())),
        }),
        Command::Commit(request) => journal_command::Operation::Commit(wire_commit(request)),
    };
    let encoded = JournalCommand {
        operation: Some(operation),
    }
    .encode_to_vec();
    if encoded.len() > MAX_COMMAND_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    Ok(encoded)
}

fn decode_command(encoded: &[u8]) -> Result<Command, StreamError> {
    let journal = JournalCommand::decode(encoded).map_err(|_| StreamError::InvalidArgument)?;
    match journal.operation.ok_or(StreamError::InvalidArgument)? {
        journal_command::Operation::Append(request) => domain_append(request).map(Command::Append),
        journal_command::Operation::Fork(request) => domain_fork(request).map(Command::Fork),
        journal_command::Operation::Trim(request) => Ok(Command::Trim {
            path: StreamPath::new(request.path)?,
            before: request.before,
            idempotency_key: required_key(request.idempotency_key)?,
        }),
        journal_command::Operation::Delete(request) => Ok(Command::Delete {
            path: StreamPath::new(request.path)?,
            idempotency_key: required_key(request.idempotency_key)?,
        }),
        journal_command::Operation::Commit(request) => domain_commit(request).map(Command::Commit),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct JournalCommand {
    #[prost(oneof = "journal_command::Operation", tags = "1, 2, 3, 4, 5")]
    operation: Option<journal_command::Operation>,
}

mod journal_command {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Operation {
        #[prost(message, tag = "1")]
        Append(crate::wire::AppendRequest),
        #[prost(message, tag = "2")]
        Fork(crate::wire::ForkRequest),
        #[prost(message, tag = "3")]
        Trim(crate::wire::TrimRequest),
        #[prost(message, tag = "4")]
        Delete(crate::wire::DeleteRequest),
        #[prost(message, tag = "5")]
        Commit(crate::wire::CommitRequest),
    }
}

fn wire_append(request: &AppendRequest) -> crate::wire::AppendRequest {
    crate::wire::AppendRequest {
        path: request.path.to_string(),
        records: request.records.clone(),
        if_tail: request.if_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    }
}

fn domain_append(request: crate::wire::AppendRequest) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new(request.path)?,
        records: request.records,
        if_tail: request.if_tail,
        idempotency_key: optional_key(request.idempotency_key)?,
    })
}

fn wire_fork(request: &ForkRequest) -> crate::wire::ForkRequest {
    crate::wire::ForkRequest {
        source: request.source.to_string(),
        destination: request.destination.to_string(),
        at_tail: request.at_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    }
}

fn domain_fork(request: crate::wire::ForkRequest) -> Result<ForkRequest, StreamError> {
    Ok(ForkRequest {
        source: StreamPath::new(request.source)?,
        destination: StreamPath::new(request.destination)?,
        at_tail: request.at_tail,
        idempotency_key: optional_key(request.idempotency_key)?,
    })
}

fn wire_commit(request: &CommitRequest) -> crate::wire::CommitRequest {
    crate::wire::CommitRequest {
        conditions: request
            .conditions
            .iter()
            .cloned()
            .map(crate::wire_codec::condition_wire)
            .collect(),
        mutations: request
            .mutations
            .iter()
            .cloned()
            .map(crate::wire_codec::mutation_wire)
            .collect(),
        idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
    }
}

fn domain_commit(request: crate::wire::CommitRequest) -> Result<CommitRequest, StreamError> {
    if request.conditions.len() > MAX_ITEMS || request.mutations.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded);
    }
    Ok(CommitRequest {
        conditions: request
            .conditions
            .into_iter()
            .map(condition_from_wire)
            .collect::<Result<_, _>>()?,
        mutations: request
            .mutations
            .into_iter()
            .map(mutation_from_wire)
            .collect::<Result<_, _>>()?,
        idempotency_key: IdempotencyKey::new(request.idempotency_key)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance;

    #[tokio::test]
    async fn local_provider_reopens_and_passes_public_conformance()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        conformance::verify(&provider)
            .await
            .map_err(std::io::Error::other)?;
        drop(provider);
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(
            reopened
                .tail(StreamPath::new("conformance/source")?)
                .await?,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_provider_excludes_a_second_process_owner_and_repairs_a_torn_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert!(matches!(
            LocalStream::open(directory.path(), LocalStreamLimits::default()).await,
            Err(LocalStreamError::AlreadyOpen)
        ));
        provider
            .append(AppendRequest {
                path: StreamPath::new("durable")?,
                records: vec![Bytes::from_static(b"one")],
                if_tail: Some(0),
                idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"append"))?),
            })
            .await?;
        drop(provider);
        let journal = directory.path().join("stream.journal");
        let valid_length = std::fs::metadata(&journal)?.len();
        OpenOptions::new()
            .append(true)
            .open(&journal)?
            .write_all(&[9, 8, 7])?;
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(StreamPath::new("durable")?).await?, 1);
        drop(reopened);
        assert_eq!(std::fs::metadata(journal)?.len(), valid_length);
        Ok(())
    }

    #[tokio::test]
    async fn journal_capacity_rejects_before_mutating_visible_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            journal_bytes: u64::try_from(HEADER_BYTES).map_err(std::io::Error::other)?,
            ..LocalStreamLimits::default()
        };
        let provider = LocalStream::open(directory.path(), limits).await?;
        assert!(matches!(
            provider
                .append(AppendRequest {
                    path: StreamPath::new("capacity")?,
                    records: vec![Bytes::from_static(b"must-not-appear")],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await,
            Err(StreamError::Capacity)
        ));
        assert!(matches!(
            provider.tail(StreamPath::new("capacity")?).await,
            Err(StreamError::NotFound)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn complete_frame_corruption_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        provider
            .append(AppendRequest {
                path: StreamPath::new("authenticated")?,
                records: vec![Bytes::from_static(b"body")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        drop(provider);

        let journal = directory.path().join("stream.journal");
        let mut file = OpenOptions::new().read(true).write(true).open(journal)?;
        let body_offset = u64::try_from(HEADER_BYTES + 5).map_err(std::io::Error::other)?;
        file.seek(SeekFrom::Start(body_offset))?;
        file.write_all(&[0xff])?;
        file.sync_all()?;
        drop(file);

        assert!(matches!(
            LocalStream::open(directory.path(), LocalStreamLimits::default()).await,
            Err(LocalStreamError::Corrupt)
        ));
        Ok(())
    }
}
