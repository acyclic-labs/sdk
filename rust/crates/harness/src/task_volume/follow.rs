use super::{Error, FileValue, Generation, Query, Result, SCHEMA, TaskVolume, segment};
use acyclic_stream::{ReadRequest, StreamProvider};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Serializable next-record cursor. Never infer continuity from a native watch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct Cursor {
    /// Volume identity bound to its provider path.
    pub volume: [u8; 32],
    /// History incarnation, persisted in the volume header.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub epoch: Uuid,
    /// Namespace and segment format.
    pub schema: u32,
    /// Exact folder selection identity.
    pub query: [u8; 32],
    /// Next segment to examine.
    pub position: u64,
    /// Highest accepted next sequence observed; delivery never waits for durability.
    pub accepted: u64,
    /// Confirmed durable prefix at the last provider watermark observation.
    pub durable: u64,
    /// Immutable commit immediately before `position`.
    pub commit: [u8; 32],
}

impl Cursor {
    /// Incorporates a pushed provider watermark without changing the consumed cut.
    pub fn advance_watermarks(
        &mut self,
        watermarks: acyclic_stream::StreamWatermarks,
    ) -> Result<()> {
        if !super::watermarks::advance(
            self.position,
            &mut self.accepted,
            &mut self.durable,
            watermarks.accepted,
            watermarks.durable,
        ) {
            return Err(Error::ResetRequired);
        }
        Ok(())
    }
}

/// One atomic-cut snapshot; file payloads remain shared segment slices.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    /// Current matching files in typed identity order (chunk sequences are numeric).
    pub files: Vec<FileValue>,
    /// Exclusive publication cut; subsequent follow begins here.
    pub cursor: Cursor,
    /// Exact generation for a fenced publication.
    pub generation: Generation,
}

/// One matching change, or a checkpoint after a nonmatching publication.
#[derive(Debug, Serialize, Deserialize)]
pub struct Change {
    /// Logical file value; `None` advances a filtered query past unrelated records.
    pub file: Option<FileValue>,
    /// Additional file replacements in the same atomic publication.
    pub files: Vec<FileValue>,
    /// File removals in that publication.
    pub removed: Vec<super::FileKey>,
    /// Resume immediately after this observation, at its accepted commit position.
    pub cursor: Cursor,
}

impl Change {
    /// A cursor-only observation contains no presentation changes.
    pub fn is_checkpoint(&self) -> bool {
        self.file.is_none() && self.files.is_empty() && self.removed.is_empty()
    }
}

/// Backpressured push follow; no task or unbounded notification queue is spawned.
pub type ChangeStream = acyclic_stream::BoxProviderStream<'static, Result<Change>>;

/// Observe returns a replacement snapshot on missing or incompatible cursors.
pub struct Observation {
    /// Present for an initial query or explicit reset.
    pub snapshot: Option<Snapshot>,
    /// The supplied cursor or cached head was incompatible with this history or selection.
    pub reset: bool,
    /// Changes after the supplied cursor or snapshot's atomic cut.
    pub changes: ChangeStream,
}

pub(crate) fn query_id(query: &Query) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&serde_json::to_vec(query).map_err(|_| Error::InvalidSegment)?).as_bytes())
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    pub(super) fn cursor(
        &self,
        query: &Query,
        generation: Generation,
        watermarks: acyclic_stream::StreamWatermarks,
    ) -> Result<Cursor> {
        if watermarks.durable > watermarks.accepted || generation.position > watermarks.accepted {
            return Err(Error::ResetRequired);
        }
        Ok(Cursor {
            volume: generation.volume,
            epoch: generation.epoch,
            schema: SCHEMA,
            query: query_id(query)?,
            position: generation.position,
            accepted: watermarks.accepted,
            durable: watermarks.durable.min(generation.position),
            commit: generation.commit,
        })
    }

    // Exact files and a follow cursor share one index observation. The index is
    // a leaf lock; provider IO is always outside it.
    pub(crate) async fn snapshot_files(
        &self,
        keys: &[super::FileKey],
        query: &Query,
    ) -> Result<Snapshot> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let files = keys
            .iter()
            .filter_map(|key| index.files.get(key))
            .map(|stored| stored.file.clone())
            .collect();
        let generation = self.generation(&index);
        drop(index);
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(Snapshot {
            files,
            cursor: self.cursor(query, generation, watermarks)?,
            generation,
        })
    }

    /// Captures a finite snapshot and cursor under one publication cut.
    pub async fn snapshot(&self, query: &Query, maximum_files: usize) -> Result<Snapshot> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let files = if matches!(
            query,
            Query::TaskView { .. } | Query::TaskSession { .. } | Query::TaskOverviews
        ) {
            super::recent::snapshot(&index, query, maximum_files)?
        } else if let Query::CommandSession { session, recent } = query {
            super::command_query::snapshot(&index, session, *recent, maximum_files)?
        } else {
            let mut files = Vec::new();
            for range in query.ranges() {
                for (_, file) in index
                    .files
                    .range(range)
                    .filter(|(_, file)| query.snapshot_matches_file(&file.file))
                {
                    if files.len() == maximum_files {
                        return Err(Error::LimitExceeded);
                    }
                    files.push(file.file.clone());
                }
            }
            files
        };
        let generation = self.generation(&index);
        drop(index);
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(Snapshot {
            files,
            cursor: self.cursor(query, generation.clone(), watermarks)?,
            generation,
        })
    }

    pub(super) async fn validate_cursor(&self, query: &Query, cursor: &Cursor) -> Result<()> {
        let generation = self.generation(&*self.index.lock().await);
        let tail = self.provider.watermarks(self.path.clone()).await?.accepted;
        if cursor.volume != generation.volume
            || cursor.epoch != generation.epoch
            || cursor.schema != SCHEMA
            || cursor.query != query_id(query)?
            || cursor.position == 0
            || cursor.position > tail
            || cursor.accepted > tail
            || cursor.accepted < cursor.position
            || cursor.durable > cursor.accepted
        {
            return Err(Error::ResetRequired);
        }
        self.validate_anchor(cursor.position, &cursor.commit).await
    }

    pub(super) async fn validate_anchor(&self, position: u64, commit: &[u8; 32]) -> Result<()> {
        let sequence = position.checked_sub(1).ok_or(Error::ResetRequired)?;
        let mut records = self
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from: sequence,
                limit: 1,
            })
            .await?;
        let record = records.next().await.ok_or(Error::ResetRequired)??;
        if record.sequence != sequence || record.commit_id.as_bytes() != commit {
            return Err(Error::ResetRequired);
        }
        Ok(())
    }

    /// Replays from a durable cursor and remains live without a snapshot/watch race.
    pub async fn follow(&self, query: Query, cursor: Cursor) -> Result<ChangeStream> {
        self.validate_cursor(&query, &cursor).await?;
        let records = self
            .provider
            .follow_checked(
                self.path.clone(),
                cursor.position,
                acyclic_stream::CommitId::from_bytes(cursor.commit),
            )
            .await?;
        Ok(record_changes(query, cursor, records))
    }

    /// Starts or resumes one query, returning an explicit reset with replacement snapshot.
    pub async fn observe(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
    ) -> Result<Observation> {
        let supplied = cursor.is_some();
        if let Some(cursor) = cursor {
            match self.follow(query.clone(), cursor).await {
                Ok(changes) => {
                    return Ok(Observation {
                        snapshot: None,
                        reset: false,
                        changes,
                    });
                }
                Err(Error::ResetRequired) => {}
                Err(error) => return Err(error),
            }
        }
        if !supplied {
            match self
                .snapshot_follow(query.clone(), maximum_files, false)
                .await
            {
                Ok(observation) => return Ok(observation),
                Err(Error::ResetRequired) => {}
                Err(error) => return Err(error),
            }
        }
        // Rebuild outside the shared lock. The reconstructed index is still a
        // fenced immutable cut; a second loss returns ResetRequired to the caller.
        let fresh = Self::open(self.provider.clone(), self.path.clone()).await?;
        #[cfg(acyclic_harness_loom)]
        let replace =
            !super::model_checking::enabled(super::model_checking::Fault::RetainObsoleteIndex);
        #[cfg(not(acyclic_harness_loom))]
        let replace = true;
        if replace {
            *self.index.lock().await = fresh.index.into_inner();
        }
        self.snapshot_follow(query, maximum_files, true).await
    }

    async fn snapshot_follow(
        &self,
        query: Query,
        maximum_files: usize,
        reset: bool,
    ) -> Result<Observation> {
        let snapshot = self.snapshot(&query, maximum_files).await?;
        let cursor = snapshot.cursor.clone();
        #[cfg(acyclic_harness_loom)]
        let cursor = if super::model_checking::enabled(super::model_checking::Fault::ResampleFollow)
        {
            self.snapshot(&query, maximum_files).await?.cursor
        } else {
            cursor
        };
        let changes = self.follow(query, cursor).await?;
        Ok(Observation {
            snapshot: Some(snapshot),
            reset,
            changes,
        })
    }
}

// One fold for provider follows and multiplexed client follows. Callers open a
// checked record stream at this cursor; no file decoding or membership policy
// is duplicated by the client transport.
pub(crate) fn record_changes(
    query: Query,
    cursor: Cursor,
    records: acyclic_stream::RecordStream,
) -> ChangeStream {
    let stream = futures::stream::try_unfold(
        (records, query, cursor),
        |(mut records, query, mut cursor)| async move {
            let Some(record) = records.next().await else {
                return Err(Error::ResetRequired);
            };
            let record = record?;
            if record.sequence != cursor.position {
                return Err(Error::ResetRequired);
            }
            let file = segment::decode(&record.value)?;
            cursor.position += 1;
            cursor.accepted = cursor.accepted.max(cursor.position);
            cursor.commit = *record.commit_id.as_bytes();
            let file = query.matches_file(&file).then_some(file);
            let (file, removed) = if query == Query::ActiveAgents {
                match file {
                    Some(file) if !super::ChildStatus::parse(&file.bytes)?.active() => {
                        (None, vec![file.key])
                    }
                    file => (file, Vec::new()),
                }
            } else {
                (file, Vec::new())
            };
            let change = Change {
                file,
                files: Vec::new(),
                removed,
                cursor: cursor.clone(),
            };
            Ok(Some((change, (records, query, cursor))))
        },
    );
    Box::pin(stream)
}
