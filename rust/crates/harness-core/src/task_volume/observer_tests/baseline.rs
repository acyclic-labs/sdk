// Historical observer layout and constructor/read path, before index sharing.
use super::*;
use acyclic_stream::StreamWatermarks;

pub(super) struct OldVolume {
    provider: Arc<MemoryStream>,
    path: StreamPath,
    index: Mutex<Index>,
}

impl OldVolume {
    pub async fn open_existing(provider: Arc<MemoryStream>, path: StreamPath) -> Result<Self> {
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

    fn generation(&self, index: &Index) -> Generation {
        Generation {
            volume: index.volume,
            epoch: index.epoch,
            position: index.next,
            commit: *index.commit.as_bytes(),
        }
    }

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
}

impl OldVolume {
    // Lock order: index only. Provider calls and stream polls never hold it.
    // Every staged record is applied only to its captured generation.
    pub(super) async fn refresh(&self) -> Result<StreamWatermarks> {
        self.refresh_to_tail::<true>().await
    }

    // RESUME=false is the model's deliberately broken early-success variant.
    pub(super) async fn refresh_to_tail<const RESUME: bool>(&self) -> Result<StreamWatermarks> {
        Ok(self.refresh_bounded::<RESUME>(u64::MAX).await?.0)
    }

    // A page spends at most this many suffix records. Provider I/O remains
    // outside the index lock, including at every replay continuation.
    pub(super) async fn refresh_bounded<const RESUME: bool>(
        &self,
        maximum_records: u64,
    ) -> Result<(StreamWatermarks, bool)> {
        if maximum_records == 0 {
            return Err(Error::LimitExceeded);
        }
        let mut remaining = maximum_records;
        let mut expected = {
            let index = self.index.lock().await;
            self.generation(&index)
        };
        let mut watermarks = self.provider.watermarks(self.path.clone()).await?;
        if watermarks.durable > watermarks.accepted || watermarks.accepted < expected.position {
            return Err(Error::ResetRequired);
        }
        let tail = watermarks.accepted;
        loop {
            let mut records = self
                .provider
                .read(ReadRequest {
                    path: self.path.clone(),
                    from: expected.position - 1,
                    limit: u32::try_from(
                        (tail - expected.position)
                            .min(remaining)
                            .min(acyclic_stream::MAX_ITEMS as u64 - 1)
                            + 1,
                    )
                    .map_err(|_| Error::LimitExceeded)?,
                })
                .await?;
            let anchor = records.next().await.ok_or(Error::ResetRequired)??;
            item_examined();
            if anchor.sequence != expected.position - 1
                || anchor.commit_id.as_bytes() != &expected.commit
            {
                return Err(Error::ResetRequired);
            }
            let before = expected.position;
            let mut peer_advanced = false;
            while let Some(record) = records.next().await {
                item_examined();
                let record = record?;
                if record.sequence >= tail {
                    break;
                }
                if record.sequence != expected.position {
                    return Err(Error::ResetRequired);
                }
                remaining -= 1;
                let mut index = self.index.lock().await;
                let current = self.generation(&index);
                if current != expected {
                    expected = advanced(current, &expected)?;
                    peer_advanced = true;
                    break;
                }
                let file = segment::decode(&record.value)?;
                validate_file(&index, &file)?;
                let next = index.next.checked_add(1).ok_or(Error::LimitExceeded)?;
                apply(&mut index, file, next);
                index.commit = record.commit_id;
                index.next = next;
                expected = self.generation(&index);
            }
            let current = {
                let index = self.index.lock().await;
                self.generation(&index)
            };
            if current != expected {
                expected = advanced(current, &expected)?;
                peer_advanced = true;
            }
            if complete::<RESUME>(expected.position, tail, peer_advanced) {
                watermarks.accepted = watermarks.accepted.max(expected.position);
                return Ok((watermarks, true));
            }
            if remaining == 0 {
                return Ok((watermarks, false));
            }
            if expected.position == before {
                return Err(Error::ResetRequired);
            }
        }
    }
}

fn complete<const RESUME: bool>(position: u64, tail: u64, peer_advanced: bool) -> bool {
    position >= tail || (!RESUME && peer_advanced)
}

pub(super) fn advanced(current: Generation, expected: &Generation) -> Result<Generation> {
    if current.volume != expected.volume
        || current.epoch != expected.epoch
        || current.position <= expected.position
    {
        return Err(Error::ResetRequired);
    }
    // Discard the staged suffix and resume at the validated shared generation.
    // A partial advance does not satisfy the accepted tail captured at entry.
    Ok(current)
}
