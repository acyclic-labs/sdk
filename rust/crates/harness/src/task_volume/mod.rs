//! Logical task files over single-event appends and immutable segment views.
mod child;
pub use child::{ChildFile, ChildState, ChildStatus};

pub(crate) mod command_query;
mod durability;
mod files;
mod follow;
mod heads;
#[cfg(acyclic_harness_loom)]
pub mod model_checking;
mod page;
mod plan_name;
mod publication;
mod recent;
pub(crate) use publication::publish;
mod refresh;
mod schema;
mod segment;
mod source;
mod watermarks;
pub use files::TaskFiles;
pub use follow::{Change, ChangeStream, Cursor, Observation, Snapshot};
pub(crate) use follow::{query_id, record_changes};
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
type FileIndex = crate::test_support::TaskVolumeFiles<StoredFile>;
#[cfg(not(feature = "test-support"))]
type FileIndex = std::collections::BTreeMap<FileKey, StoredFile>;

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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct Generation {
    /// Volume identity, including its provider path.
    pub volume: [u8; 32],
    /// Durable history incarnation.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
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
    /// Original filesystem write operation, when supplied by a file query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<crate::OperationId>,
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
            if !matches!(header.schema, 2 | 3 | 4 | SCHEMA) {
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
            publication: None,
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

struct StoredFile {
    file: FileValue,
    position: u64,
}

#[derive(Default)]
struct Index {
    volume: [u8; 32],
    epoch: Uuid,
    next: u64,
    commit: CommitId,
    files: FileIndex,
    executions: BTreeMap<(Option<Component>, u64), FileKey>,
    pending_commands: BTreeMap<Option<Component>, BTreeSet<FileKey>>,
    view_order: BTreeMap<(Component, u8, u64), FileKey>,
    view_revisions: BTreeMap<FileKey, (u8, u64)>,
    overviews: BTreeMap<Component, FileKey>,
}

/// A disposable index over one durable private task volume.
/// Hosts authorize writes/queries with the volume's existing access decision.
pub struct TaskVolume<P: ?Sized> {
    provider: Arc<P>,
    path: StreamPath,
    // Lock order: index only; no provider I/O or stream poll holds this lock.
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
        if !matches!(header.schema, 2 | 3 | 4 | SCHEMA) {
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
        volume.refresh().await?;
        Ok(volume)
    }

    /// Current publication fence without copying the file index.
    pub async fn head(&self) -> Result<Generation> {
        self.refresh().await?;
        let index = self.index.lock().await;
        Ok(self.generation(&index))
    }

    /// Current query cut without copying the materialized files.
    pub async fn cut(&self, query: &Query) -> Result<Cursor> {
        let mut watermarks = self.refresh().await?;
        let index = self.index.lock().await;
        watermarks.accepted = watermarks.accepted.max(index.next);
        self.cursor(query, self.generation(&index), watermarks)
    }

    fn generation(&self, index: &Index) -> Generation {
        Generation {
            volume: index.volume,
            epoch: index.epoch,
            position: index.next,
            commit: *index.commit.as_bytes(),
        }
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
        self.refresh().await?;
        let index = self.index.lock().await;
        if expected != self.generation(&index) {
            return Err(Error::Conflict(self.generation(&index)));
        }
        if create_only && index.files.contains_key(&write.key) {
            return Err(Error::ImmutableConflict(write.key.clone()));
        }
        validate_file(&index, write)?;
        if index
            .files
            .get(&write.key)
            .is_some_and(|stored| &stored.file == write)
        {
            return Ok(self.generation(&index));
        }
        let next = expected
            .position
            .checked_add(1)
            .ok_or(Error::LimitExceeded)?;
        let (segment, files) = segment::encode(write, work)?;
        #[cfg(acyclic_harness_loom)]
        if model_checking::enabled(model_checking::Fault::OmitAuthorityConditions) {
            conditions.clear();
        }
        let fence = CommitCondition::Head {
            path: self.path.clone(),
            expected: expected.position,
            commit_id: CommitId::from_bytes(expected.commit),
        };
        #[cfg(acyclic_harness_loom)]
        let fence = if model_checking::enabled(model_checking::Fault::ResamplePublicationFence) {
            CommitCondition::Tail {
                path: self.path.clone(),
                expected: self.provider.watermarks(self.path.clone()).await?.accepted,
            }
        } else {
            fence
        };
        conditions.push(fence);
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
        #[cfg(acyclic_harness_loom)]
        let deadline_unix_millis = if model_checking::enabled(model_checking::Fault::IgnoreDeadline)
        {
            None
        } else {
            deadline_unix_millis
        };
        let outcome = if let Some(deadline) = deadline_unix_millis {
            self.provider.commit_before(request, deadline).await?
        } else {
            self.provider.commit(request).await?
        };
        match outcome {
            CommitOutcome::Committed(envelope) => {
                let mut index = self.index.lock().await;
                let unchanged = self.generation(&index) == expected;
                #[cfg(acyclic_harness_loom)]
                let unchanged = unchanged
                    || model_checking::enabled(model_checking::Fault::OverwriteRebuiltIndex);
                if unchanged {
                    apply(&mut index, files, next);
                    index.next = next;
                    index.commit = envelope.commit_id;
                }
                Ok(Generation {
                    volume: expected.volume,
                    epoch: expected.epoch,
                    position: next,
                    commit: *envelope.commit_id.as_bytes(),
                })
            }
            CommitOutcome::Conflict(_) => {
                self.refresh().await?;
                let index = self.index.lock().await;
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
    if matches!(
        file.key,
        FileKey::Child {
            file: ChildFile::Status,
            ..
        }
    ) {
        ChildStatus::parse(&file.bytes)?;
    }
    file.validate()?;
    if !file.key.mutable()
        && index
            .files
            .get(&file.key)
            .is_some_and(|old| &old.file != file)
    {
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

fn apply(index: &mut Index, file: FileValue, position: u64) {
    recent::record(index, &file, position);
    let owner = command_query::owner(&file.key);
    if command_query::is_result(&file.key) && !index.files.contains_key(&file.key) {
        index
            .executions
            .insert((owner.clone(), position), file.key.clone());
        if let Some(pending) = index.pending_commands.get_mut(&owner) {
            for make in [TaskFile::Request, TaskFile::ExecutionClaim] {
                if let Some(key) = command_query::related(&file.key, make) {
                    pending.remove(&key);
                }
            }
            if pending.is_empty() {
                index.pending_commands.remove(&owner);
            }
        }
    } else if let Some(result) = command_query::related(&file.key, TaskFile::ExecutionResult) {
        if !index.files.contains_key(&result) {
            index
                .pending_commands
                .entry(owner)
                .or_default()
                .insert(file.key.clone());
        }
    }
    index
        .files
        .insert(file.key.clone(), StoredFile { file, position });
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl<P: StreamProvider + 'static> TaskVolume<P> {
    pub(crate) async fn reopen_erased(&self) -> Result<TaskVolume<dyn StreamProvider>> {
        let provider: Arc<dyn StreamProvider> = self.provider.clone();
        TaskVolume::open(provider, self.path.clone()).await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod cursor_version_tests;
