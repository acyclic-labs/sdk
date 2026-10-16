//! Logical task files over single-event appends and immutable segment views.

pub(crate) mod command_query;
mod files;
mod follow;
mod heads;
mod page;
mod plan_name;
mod schema;
mod segment;
mod source;
mod watermarks;
pub use files::TaskFiles;
pub use follow::{Change, ChangeStream, Cursor, Observation, Snapshot};
pub use heads::{HeadFile, HeadSnapshot};
pub use plan_name::PlanName;
pub use schema::{
    Channel, Component, FileKey, InputPath, Owner, Query, SCHEMA, TaskFile, ToolFile,
};
pub use source::FileQuerySource;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::NativeWork;
use crate::sync::Mutex;
use acyclic_stream::{
    AppendRequest, CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitRequest,
    IdempotencyKey, OwnedBytes, ReadRequest, StreamError, StreamPath, StreamProvider,
};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type NativeWork<'a> = Option<&'a ()>;

#[cfg(feature = "test-support")]
use crate::test_support::TaskVolumeFiles as FileIndex;
#[cfg(not(feature = "test-support"))]
type FileIndex = std::collections::BTreeMap<FileKey, FileValue>;

#[inline]
fn item_examined() {
    #[cfg(feature = "test-support")]
    crate::test_support::task_volume_item_examined();
}

/// Task-volume failure; reset requires a fresh bounded snapshot.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Private task volume access was not granted to this owner.
    #[error("task-volume access denied")]
    Unauthorized,
    /// Authorization, work or storage failure accessing volume files.
    #[cfg(feature = "filesystem")]
    #[error(transparent)]
    FileRead(#[from] crate::Error),
    /// Underlying durable storage failure.
    #[error(transparent)]
    Storage(StreamError),
    /// Noncanonical logical path or identifier.
    #[error("invalid task-volume path")]
    InvalidPath,
    /// Unsupported or malformed durable bytes.
    #[error("invalid task-volume segment")]
    InvalidSegment,
    /// Publication, snapshot or segment bound exceeded.
    #[error("task-volume limit exceeded")]
    LimitExceeded,
    /// An immutable logical file was assigned conflicting content or channel.
    #[error("immutable task file conflict: {0:?}")]
    ImmutableConflict(FileKey),
    /// A new chunk would move its task/tool stream backwards.
    #[error("task chunk sequence conflict: {0:?}")]
    SequenceConflict(FileKey),
    /// Another publisher advanced the volume; revalidate execution ownership.
    #[error("task-volume generation conflict: {0:?}")]
    Conflict(Generation),
    /// An execution-owner condition failed without publishing any task files.
    #[error("task-volume execution fence conflict")]
    FenceConflict,
    /// Cursor identity or position cannot be resumed in this history/query.
    #[error("task-volume query reset required")]
    ResetRequired,
}

/// Task-volume operation result.
pub type Result<T> = std::result::Result<T, Error>;

impl From<StreamError> for Error {
    fn from(error: StreamError) -> Self {
        match error {
            StreamError::NotFound | StreamError::OutOfRange | StreamError::PrefixNotRetained => {
                Self::ResetRequired
            }
            error => Self::Storage(error),
        }
    }
}

/// Exact publication fence. A scheduler must retain its own execution lease too.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Generation {
    /// Volume identity, including its provider path.
    pub volume: [u8; 32],
    /// Durable history incarnation.
    pub epoch: Uuid,
    /// Next append-only segment position.
    pub position: u64,
    /// Immutable identity at the cut, detecting reuse of an accepted position after loss.
    pub commit: [u8; 32],
}

/// One logical file backed by a shared immutable slice of its publication segment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileValue {
    /// Logical immutable identity or mutable state location.
    pub key: FileKey,
    /// Assistant channel discriminator, required only for assistant chunks.
    pub channel: Option<Channel>,
    /// Zero-copy segment view, retaining original allocation custody.
    pub bytes: OwnedBytes,
}

/// One file event supplied to its own append.
pub type Write = FileValue;

impl FileValue {
    /// Decodes a platform inbox/event record for an actor handler. The initial
    /// schema/volume declaration has no file event and returns `None`.
    pub fn from_record(record: &acyclic_stream::Record) -> Result<Option<Self>> {
        if record.sequence == 0 {
            let header: Header =
                serde_json::from_slice(&record.value).map_err(|_| Error::InvalidSegment)?;
            if !matches!(header.schema, 2 | 3 | SCHEMA) {
                return Err(Error::InvalidSegment);
            }
            Ok(None)
        } else {
            segment::decode(&record.value).map(Some)
        }
    }

    /// Constructs an ordinary file write.
    pub fn new(key: FileKey, bytes: impl Into<OwnedBytes>) -> Self {
        Self {
            key,
            channel: None,
            bytes: bytes.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        if matches!(
            self.key,
            FileKey::Task {
                file: TaskFile::TextHead { .. },
                ..
            }
        ) {
            return Err(Error::InvalidSegment);
        }
        let chunk = matches!(
            self.key,
            FileKey::Task {
                file: TaskFile::Chunk { .. },
                ..
            }
        );
        if chunk != self.channel.is_some() {
            return Err(Error::InvalidSegment);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Header {
    volume: Uuid,
    epoch: Uuid,
    schema: u32,
}

#[derive(Default)]
struct Index {
    volume: [u8; 32],
    epoch: Uuid,
    next: u64,
    commit: CommitId,
    files: FileIndex,
    executions: BTreeMap<(Option<Component>, u64), FileKey>,
    pending_commands: BTreeSet<FileKey>,
}

/// A disposable index over one durable private task volume.
/// Hosts authorize writes/queries with the volume's existing access decision.
pub struct TaskVolume<P: ?Sized> {
    provider: Arc<P>,
    path: StreamPath,
    // Lock order: index -> provider visibility -> provider state; never held across commit acknowledgement.
    index: Mutex<Index>,
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Authenticated provider path bound to this volume's history identity.
    pub fn path(&self) -> &StreamPath {
        &self.path
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn shares_provider(&self, provider: &Arc<P>) -> bool {
        Arc::ptr_eq(&self.provider, provider)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn file_volume(&self, key: &FileKey) -> Result<Self> {
        // A separate file stream cannot contend with unrelated parent-volume writes.
        // Release the parent index before opening/locking the child volume.
        let identity = {
            let index = self.index.lock().await;
            let mut hash = blake3::Hasher::new();
            hash.update(&index.volume);
            hash.update(index.epoch.as_bytes());
            hash.update(key.path().as_bytes());
            hash.finalize()
        };
        let path = StreamPath::new(format!(
            "{}/files/{}",
            self.path.as_str(),
            identity.to_hex()
        ))?;
        Self::open(self.provider.clone(), path).await
    }
    /// Opens or atomically initializes a volume, then reconstructs its logical index.
    pub async fn open(provider: Arc<P>, path: StreamPath) -> Result<Self> {
        let initialize = match provider.watermarks(path.clone()).await {
            Ok(acyclic_stream::StreamWatermarks { accepted: 0, .. })
            | Err(StreamError::NotFound) => true,
            Ok(_) => false,
            Err(error) => return Err(error.into()),
        };
        if initialize {
            let header = Header {
                volume: Uuid::new_v4(),
                epoch: Uuid::new_v4(),
                schema: SCHEMA,
            };
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![
                        serde_json::to_vec(&header)
                            .map_err(|_| Error::InvalidSegment)?
                            .into(),
                    ],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await?;
        }
        Self::open_existing(provider, path).await
    }

    /// Opens a published volume without creating or writing any state.
    pub async fn open_existing(provider: Arc<P>, path: StreamPath) -> Result<Self> {
        let mut records = provider
            .read(ReadRequest {
                path: path.clone(),
                from: 0,
                limit: 1,
            })
            .await?;
        let record = records.next().await.ok_or(Error::ResetRequired)??;
        if record.sequence != 0 {
            return Err(Error::InvalidSegment);
        }
        let header: Header =
            serde_json::from_slice(&record.value).map_err(|_| Error::InvalidSegment)?;
        if !matches!(header.schema, 2 | 3 | SCHEMA) {
            return Err(Error::InvalidSegment);
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(path.as_str().as_bytes());
        hasher.update(header.volume.as_bytes());
        let scope = *hasher.finalize().as_bytes();
        let volume = Self {
            provider,
            path,
            index: Mutex::new(Index {
                volume: scope,
                epoch: header.epoch,
                next: 1,
                commit: record.commit_id,
                ..Index::default()
            }),
        };
        volume.refresh(&mut *volume.index.lock().await).await?;
        Ok(volume)
    }

    fn generation(&self, index: &Index) -> Generation {
        Generation {
            volume: index.volume,
            epoch: index.epoch,
            position: index.next,
            commit: *index.commit.as_bytes(),
        }
    }

    async fn refresh(&self, index: &mut Index) -> Result<()> {
        let tail = self.provider.watermarks(self.path.clone()).await?.accepted;
        while !self
            .refresh_to(index, tail, acyclic_stream::MAX_ITEMS as usize - 1)
            .await?
        {}
        Ok(())
    }

    // One anchored, bounded suffix read. The index lock owns replay order;
    // callers may yield between slices without retaining a second snapshot.
    async fn refresh_to(
        &self,
        index: &mut Index,
        tail: u64,
        maximum_records: usize,
    ) -> Result<bool> {
        if tail < index.next {
            return Err(Error::ResetRequired);
        }
        let suffix = (tail - index.next)
            .min(maximum_records as u64)
            .min(acyclic_stream::MAX_ITEMS as u64 - 1);
        let mut records = self
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from: index.next - 1,
                limit: u32::try_from(suffix + 1).map_err(|_| Error::LimitExceeded)?,
            })
            .await?;
        // Anchor and suffix belong to one immutable read: a replaced accepted
        // history cannot be spliced into the materialized index.
        let anchor = records.next().await.ok_or(Error::ResetRequired)??;
        item_examined();
        if anchor.sequence != index.next - 1 || anchor.commit_id != index.commit {
            return Err(Error::ResetRequired);
        }
        let before = index.next;
        let through = index.next + suffix;
        while let Some(record) = records.next().await {
            item_examined();
            let record = record?;
            if record.sequence != index.next || record.sequence >= through {
                return Err(Error::ResetRequired);
            }
            let file = segment::decode(&record.value)?;
            validate_file(index, &file)?;
            apply(index, file);
            index.commit = record.commit_id;
            index.next += 1;
        }
        if index.next == before && index.next < tail {
            return Err(Error::ResetRequired);
        }
        Ok(index.next == tail)
    }

    /// Accepts one file event in its own append, without a batching boundary.
    pub async fn append(&self, expected: Generation, write: &Write) -> Result<Generation> {
        self.append_if(expected, write, Vec::new(), None, None)
            .await
    }

    /// Accepts one delta, tool event or status change in its own append.
    /// Native callers supply their original ledger for packing and retained bytes.
    pub async fn append_if(
        &self,
        expected: Generation,
        write: &Write,
        conditions: Vec<CommitCondition>,
        deadline_unix_millis: Option<u64>,
        work: NativeWork<'_>,
    ) -> Result<Generation> {
        self.publish(
            expected,
            write,
            conditions,
            deadline_unix_millis,
            false,
            work,
        )
        .await
    }

    /// Creates one file only if absent at the exact query generation. Unlike an
    /// idempotent append, an existing identical file is still a failed creation.
    pub async fn create(&self, expected: Generation, write: &Write) -> Result<Generation> {
        self.publish(expected, write, Vec::new(), None, true, None)
            .await
    }

    async fn publish(
        &self,
        expected: Generation,
        write: &Write,
        mut conditions: Vec<CommitCondition>,
        deadline_unix_millis: Option<u64>,
        create_only: bool,
        work: NativeWork<'_>,
    ) -> Result<Generation> {
        let mut index = self.index.lock().await;
        self.refresh(&mut index).await?;
        if expected != self.generation(&index) {
            return Err(Error::Conflict(self.generation(&index)));
        }
        if create_only && index.files.contains_key(&write.key) {
            return Err(Error::ImmutableConflict(write.key.clone()));
        }
        validate_file(&index, write)?;
        if index.files.get(&write.key) == Some(write) {
            return Ok(self.generation(&index));
        }
        let (segment, files) = segment::encode(write, work)?;
        conditions.push(CommitCondition::Head {
            path: self.path.clone(),
            expected: expected.position,
            commit_id: CommitId::from_bytes(expected.commit),
        });
        let request = CommitRequest {
            conditions,
            mutations: vec![CommitMutation::Append {
                path: self.path.clone(),
                records: vec![segment],
            }],
            idempotency_key: IdempotencyKey::new(Uuid::new_v4().as_bytes().to_vec())?,
        };
        // Readers must be able to project accepted records while the provider's
        // append acknowledgement is still waiting for its durability barrier.
        drop(index);
        let outcome = if let Some(deadline) = deadline_unix_millis {
            self.provider.commit_before(request, deadline).await?
        } else {
            self.provider.commit(request).await?
        };
        let mut index = self.index.lock().await;
        match outcome {
            CommitOutcome::Committed(envelope) => {
                if self.generation(&index) == expected {
                    apply(&mut index, files);
                    index.next = expected.position + 1;
                    index.commit = envelope.commit_id;
                }
                Ok(Generation {
                    volume: expected.volume,
                    epoch: expected.epoch,
                    position: expected.position + 1,
                    commit: *envelope.commit_id.as_bytes(),
                })
            }
            CommitOutcome::Conflict(_) => {
                self.refresh(&mut index).await?;
                if expected == self.generation(&index) {
                    Err(Error::FenceConflict)
                } else {
                    Err(Error::Conflict(self.generation(&index)))
                }
            }
        }
    }
}

fn validate_file(index: &Index, file: &FileValue) -> Result<()> {
    file.validate()?;
    if !file.key.mutable() && index.files.get(&file.key).is_some_and(|old| old != file) {
        return Err(Error::ImmutableConflict(file.key.clone()));
    }
    if !index.files.contains_key(&file.key)
        && let Some((first, last, sequence)) = file.key.sequence_range()
        && index
            .files
            .range(first..=last)
            .next_back()
            .and_then(|(key, _)| key.sequence_range().map(|(_, _, n)| n))
            .is_some_and(|previous| sequence <= previous)
    {
        return Err(Error::SequenceConflict(file.key.clone()));
    }
    Ok(())
}

fn apply(index: &mut Index, file: FileValue) {
    if command_query::is_result(&file.key) && !index.files.contains_key(&file.key) {
        let owner = match &file.key {
            FileKey::Session { session, .. } => Some(session.clone()),
            _ => None,
        };
        index
            .executions
            .insert((owner, index.next), file.key.clone());
        for make in [TaskFile::Request, TaskFile::ExecutionClaim] {
            if let Some(key) = command_query::related(&file.key, make) {
                index.pending_commands.remove(&key);
            }
        }
    } else if let Some(result) = command_query::related(&file.key, TaskFile::ExecutionResult) {
        if !index.files.contains_key(&result) {
            index.pending_commands.insert(file.key.clone());
        }
    }
    index.files.insert(file.key.clone(), file);
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl<P: StreamProvider + 'static> TaskVolume<P> {
    pub(crate) async fn reopen_erased(&self) -> Result<TaskVolume<dyn StreamProvider>> {
        let provider: Arc<dyn StreamProvider> = self.provider.clone();
        TaskVolume::open(provider, self.path.clone()).await
    }
}
