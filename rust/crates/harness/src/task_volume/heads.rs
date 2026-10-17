use super::{
    Cursor, Error, FileKey, FileValue, Generation, Index, Query, Result, Snapshot, TaskFile,
    TaskVolume, apply, segment, validate_file,
};
use acyclic_stream::{OwnedBytes, ReadRequest, StreamProvider};
use futures::StreamExt as _;
use std::collections::BTreeMap;

/// A logical materialized file whose content is a sequence of shared segment slices.
#[derive(Clone, Debug, PartialEq)]
pub struct HeadFile {
    /// Status, tool, plan, or derived `turns/<n>/{text,reasoning}.md` location.
    pub key: FileKey,
    /// Original assistant channel, retained for bounded raw file selections.
    pub channel: Option<super::Channel>,
    /// Original write identity for goal acknowledgements and derived projections.
    pub publication: Option<crate::OperationId>,
    /// File content in order, without copying or reserializing accumulated text.
    pub segments: Vec<OwnedBytes>,
    /// Total logical file length.
    pub length: u64,
    /// Last assistant chunk included in a text head.
    pub through_sequence: Option<u64>,
}

/// Materialized files at one cut, usable for a tier transition followed from `cursor`.
pub struct HeadSnapshot {
    /// Latest-value ordinary files plus materialized assistant text heads.
    pub files: Vec<HeadFile>,
    /// Exact cut covering every returned head.
    pub cursor: Cursor,
    /// Publication generation at that cut.
    pub generation: Generation,
}

impl FileValue {
    pub(crate) fn head_key(&self) -> Result<(FileKey, Option<u64>)> {
        self.validate()?;
        Ok(match (&self.key, self.channel) {
            (
                FileKey::Task {
                    task,
                    file: TaskFile::Chunk { turn, sequence },
                },
                Some(channel),
            ) => (
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::TextHead {
                        turn: *turn,
                        channel,
                    },
                },
                Some(*sequence),
            ),
            _ => (self.key.clone(), None),
        })
    }
}

impl Snapshot {
    /// Converts a selected snapshot into zero-copy materialized head files.
    pub fn into_heads(self) -> Result<HeadSnapshot> {
        let mut heads = BTreeMap::<FileKey, HeadFile>::new();
        for file in self.files {
            let (key, sequence) = file.head_key()?;
            let head = heads.entry(key.clone()).or_insert_with(|| HeadFile {
                key,
                publication: file.publication,
                channel: file.channel,
                segments: Vec::new(),
                length: 0,
                through_sequence: None,
            });
            head.length = head
                .length
                .checked_add(file.bytes.len() as u64)
                .ok_or(Error::LimitExceeded)?;
            head.through_sequence = sequence;
            head.segments.push(file.bytes);
        }
        Ok(HeadSnapshot {
            files: heads.into_values().collect(),
            cursor: self.cursor,
            generation: self.generation,
        })
    }
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Materializes the latest selected heads, or the exact heads at a previously pinned
    /// query cursor. The subsequent full-fidelity follow starts at the same cut.
    pub async fn heads(
        &self,
        query: &Query,
        at: Option<Cursor>,
        maximum_files: usize,
    ) -> Result<HeadSnapshot> {
        let snapshot = if let Some(cursor) = at {
            self.snapshot_at(query, cursor, maximum_files).await?
        } else {
            self.snapshot(query, maximum_files).await?
        };
        snapshot.into_heads()
    }

    /// Reads an exact immutable accepted cut. A lost/reused accepted suffix requires
    /// reset; current snapshots use the existing materialized index without replay.
    pub async fn snapshot_at(
        &self,
        query: &Query,
        cursor: Cursor,
        maximum_files: usize,
    ) -> Result<Snapshot> {
        self.validate_cursor(query, &cursor).await?;
        let mut index = Index {
            volume: cursor.volume,
            epoch: cursor.epoch,
            next: 1,
            ..Index::default()
        };
        while index.next < cursor.position {
            let mut records = self
                .provider
                .read(ReadRequest {
                    path: self.path.clone(),
                    from: index.next,
                    limit: u32::try_from(
                        (cursor.position - index.next).min(acyclic_stream::MAX_ITEMS as u64),
                    )
                    .map_err(|_| Error::LimitExceeded)?,
                })
                .await?;
            let before = index.next;
            while let Some(record) = records.next().await {
                let record = record?;
                if record.sequence >= cursor.position {
                    break;
                }
                if record.sequence != index.next {
                    return Err(Error::ResetRequired);
                }
                let files = segment::decode(&record.value)?;
                validate_file(&index, &files)?;
                let next = index.next.checked_add(1).ok_or(Error::LimitExceeded)?;
                apply(&mut index, files, next);
                index.commit = record.commit_id;
                index.next = next;
            }
            if index.next == before {
                return Err(Error::ResetRequired);
            }
        }
        // Empty heads still anchor the initialized volume header.
        if cursor.position == 1 {
            let mut records = self
                .provider
                .read(ReadRequest {
                    path: self.path.clone(),
                    from: 0,
                    limit: 1,
                })
                .await?;
            index.commit = records.next().await.ok_or(Error::ResetRequired)??.commit_id;
        }
        if index.commit.as_bytes() != &cursor.commit {
            return Err(Error::ResetRequired);
        }
        let files = if matches!(query, Query::TaskView { .. } | Query::TaskOverviews) {
            super::recent::snapshot(&index, query, maximum_files)?
        } else if let Query::CommandSession { session, recent } = query {
            super::command_query::snapshot(&index, session, *recent, maximum_files)?
        } else {
            let mut files = Vec::new();
            for range in query.ranges() {
                for (_, file) in index
                    .files
                    .range(range)
                    .filter(|(_, f)| query.snapshot_matches_file(&f.file))
                {
                    if files.len() == maximum_files {
                        return Err(Error::LimitExceeded);
                    }
                    files.push(file.file.clone());
                }
            }
            files
        };
        Ok(Snapshot {
            files,
            generation: self.generation(&index),
            cursor,
        })
    }
}
