//! Logical task files over single-event appends and immutable segment views.
mod child;
mod child_queries;
mod child_result;
pub(crate) use child::child_scope;
pub use child::{ChildFile, ChildRequest, ChildState, ChildStatus, Control};
pub use child_result::ChildTurnResult;
pub(crate) mod command_query;
#[cfg(all(test, feature = "test-support", not(target_arch = "wasm32")))]
mod derived_tests;
mod durability;
mod files;
mod follow;
mod heads;
mod listing;
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
pub(crate) use segment::record_bytes;
mod source;
mod watermarks;
pub use files::TaskFiles;
pub(crate) use files::authorize_write;
pub(crate) use follow::query_id;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) use follow::record_changes;
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
use std::{collections::BTreeMap, sync::Arc};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) requester: Option<crate::TaskId>,
    /// Original filesystem write operation, when supplied by a file query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<crate::OperationId>,
    /// Zero-copy segment view, retaining original allocation custody.
    pub bytes: OwnedBytes,
}

/// One file event supplied to its own append.
pub type Write = FileValue;

impl FileValue {
    /// Original requesting task for an owner-published child request.
    pub fn requester(&self) -> Option<crate::TaskId> {
        self.requester
    }

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
            requester: None,
            publication: None,
            bytes: bytes.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        if matches!(
            self.key,
            FileKey::Child {
                file: ChildFile::Result(0),
                ..
            }
        ) {
            return Err(Error::InvalidSegment);
        }
        if self.requester.is_some()
            && !matches!(
                self.key,
                FileKey::Child {
                    file: ChildFile::Request,
                    ..
                }
            )
        {
            return Err(Error::InvalidSegment);
        }
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

#[cfg_attr(acyclic_harness_loom, derive(Clone))]
struct StoredFile {
    file: FileValue,
    position: u64,
}

#[derive(Default)]
#[cfg_attr(acyclic_harness_loom, derive(Clone))]
struct Index {
    volume: [u8; 32],
    epoch: Uuid,
    next: u64,
    commit: CommitId,
    files: FileIndex,
    commands: command_query::Commands,
    views: recent::Views,
    child_inputs: BTreeMap<Component, Vec<FileKey>>,
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
    pub(crate) async fn reopen(&self) -> Result<Self> {
        Self::open_existing(self.provider.clone(), self.path.clone()).await
    }

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
        Self::open(self.provider.clone(), self.file_volume_path(key).await?).await
    }

    /// The stream of `key`'s file volume, which may not exist yet.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn file_volume_path(&self, key: &FileKey) -> Result<StreamPath> {
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
        Ok(StreamPath::new(format!(
            "{}/files/{}",
            self.path.as_str(),
            identity.to_hex()
        ))?)
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
        self.create_if(expected, write, Vec::new(), None, None)
            .await
    }

    /// Creates an absent file under the same owner and deadline conditions as append.
    pub async fn create_if(
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
            true,
            work,
        )
        .await
    }

    async fn publish(
        &self,
        expected: Generation,
        write: &Write,
        conditions: Vec<CommitCondition>,
        deadline_unix_millis: Option<u64>,
        create_only: bool,
        work: NativeWork<'_>,
    ) -> Result<Generation> {
        let Some((request, files, next)) = self
            .prepare_append(expected, write, conditions, create_only, work)
            .await?
        else {
            return Ok(expected);
        };
        self.commit_append(expected, request, files, next, deadline_unix_millis)
            .await
    }

    async fn prepare_append(
        &self,
        expected: Generation,
        write: &Write,
        mut conditions: Vec<CommitCondition>,
        create_only: bool,
        work: NativeWork<'_>,
    ) -> Result<Option<(CommitRequest, FileValue, u64)>> {
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
            return Ok(None);
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
        Ok(Some((request, files, next)))
    }

    async fn commit_append(
        &self,
        expected: Generation,
        request: CommitRequest,
        files: FileValue,
        next: u64,
        deadline_unix_millis: Option<u64>,
    ) -> Result<Generation> {
        #[cfg(acyclic_harness_loom)]
        let deadline_unix_millis = if model_checking::enabled(model_checking::Fault::IgnoreDeadline)
        {
            None
        } else {
            deadline_unix_millis
        };
        #[cfg(acyclic_harness_loom)]
        model_checking::publication_ready().await;
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
    if let FileKey::Child {
        reference,
        file: child_file,
    } = &file.key
    {
        if matches!(child_file, ChildFile::Delivered) {
            let (task, delivered): (crate::TaskId, u64) =
                serde_json::from_slice(&file.bytes).map_err(|_| Error::InvalidSegment)?;
            let count = index.child_inputs.get(reference).map_or(0, Vec::len) as u64;
            if delivered == 0 || delivered > count {
                return Err(Error::InvalidSegment);
            }
            if let Some(old) = index.files.get(&file.key) {
                let (previous_task, previous): (crate::TaskId, u64) =
                    serde_json::from_slice(&old.file.bytes).map_err(|_| Error::InvalidSegment)?;
                if previous_task != task || previous > delivered {
                    return Err(Error::InvalidSegment);
                }
            }
        }
        let control_key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Control,
        };
        let control = index.files.get(&control_key).map(|stored| &stored.file);
        let state = match control {
            Some(control) => ChildState::Running.apply(Control::parse(&control.bytes)?),
            None => ChildState::Running,
        };
        let terminal = matches!(state, ChildState::Archived | ChildState::Cancelled);
        if matches!(child_file, ChildFile::Status) {
            let status = ChildStatus::parse(&file.bytes)?;
            if status.state != state {
                return Err(Error::ImmutableConflict(control_key));
            }
            if let Some(previous) = index.files.get(&file.key).map(|stored| &stored.file) {
                let previous = ChildStatus::parse(&previous.bytes)?;
                if status.inputs < previous.inputs
                    || (terminal
                        && (status.task != previous.task || status.inputs != previous.inputs))
                {
                    return Err(Error::ImmutableConflict(file.key.clone()));
                }
                // The first quiescent terminal acknowledgement freezes this
                // status too. No later result or input binding can replace it.
                let archival_ack =
                    previous.state == ChildState::Cancelled && status.state == ChildState::Archived;
                if matches!(previous.state, ChildState::Archived | ChildState::Cancelled)
                    && !archival_ack
                    && index.files.get(&file.key).map(|stored| &stored.file) != Some(file)
                {
                    return Err(Error::ImmutableConflict(file.key.clone()));
                }
            }
        } else if terminal && control != Some(file) {
            let draining_result = matches!(child_file, ChildFile::Result(_) | ChildFile::Delivered)
                && index
                    .files
                    .get(&FileKey::Child {
                        reference: reference.clone(),
                        file: ChildFile::Status,
                    })
                    .is_some_and(|stored| {
                        ChildStatus::parse(&stored.file.bytes).is_ok_and(|status| {
                            !matches!(status.state, ChildState::Cancelled | ChildState::Archived)
                        })
                    });
            let archival_intent = state == ChildState::Cancelled
                && matches!(child_file, ChildFile::Control)
                && Control::parse(&file.bytes)? == Control::Archive;
            if !draining_result && !archival_intent {
                return Err(Error::ImmutableConflict(control_key));
            }
        }
        if matches!(child_file, ChildFile::Control) {
            Control::parse(&file.bytes)?;
        }
        if let ChildFile::Result(turn) = child_file {
            let result = ChildTurnResult::parse(&file.bytes)?;
            let status_key = FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Status,
            };
            let status = index.files.get(&status_key).ok_or(Error::InvalidSegment)?;
            let status = ChildStatus::parse(&status.file.bytes)?;
            if result.turn != *turn || status.task != result.task || *turn > status.inputs {
                return Err(Error::InvalidSegment);
            }
            if !index.files.contains_key(&file.key)
                && *turn > 1
                && !index.files.contains_key(&FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Result(turn - 1),
                })
            {
                return Err(Error::InvalidSegment);
            }
        }
    }
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

// The only mutation of the file index; derived views and command indexes
// fold the same replacement, never a separately maintained copy of it.
fn apply(index: &mut Index, file: FileValue, position: u64) {
    if let FileKey::Child {
        reference,
        file: ChildFile::Request | ChildFile::Inbox(_),
    } = &file.key
        && !index.files.contains_key(&file.key)
    {
        index
            .child_inputs
            .entry(reference.clone())
            .or_default()
            .push(file.key.clone());
    }
    let command = index.commands.prepare(&index.files, &file.key);
    let display = recent::Views::prepare(&file, position);
    let previous = index
        .files
        .insert(file.key.clone(), StoredFile { file, position });
    index.views.record(display, previous.as_ref());
    index.commands.record(command, position, previous.is_some());
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl<P: StreamProvider + 'static> TaskVolume<P> {
    pub(crate) async fn reopen_erased(&self) -> Result<TaskVolume<dyn StreamProvider>> {
        let provider: Arc<dyn StreamProvider> = self.provider.clone();
        TaskVolume::open(provider, self.path.clone()).await
    }
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl TaskVolume<dyn StreamProvider> {
    pub(crate) async fn reopen_erased(&self) -> Result<Self> {
        Self::open(Arc::clone(&self.provider), self.path.clone()).await
    }
}
