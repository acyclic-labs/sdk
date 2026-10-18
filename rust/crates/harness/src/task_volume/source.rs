use super::{Change, Cursor, Error, FileKey, FileValue, Observation, Query, Result, TaskVolume};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, ReadRequest, StreamProvider};
use futures::StreamExt as _;
use std::collections::BTreeMap;

/// Snapshot/cursor-follow over canonical files, independent of their physical layout.
pub trait FileQuerySource: ProviderPlatform {
    /// Opens or resumes one bounded selection. A reset supplies a new snapshot.
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> BoxProviderFuture<'_, Result<Observation>>;
}

impl<P: StreamProvider + ?Sized> FileQuerySource for TaskVolume<P> {
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> BoxProviderFuture<'_, Result<Observation>> {
        Box::pin(async move {
            if maximum_files == 0 || maximum_bytes == 0 {
                return Err(Error::LimitExceeded);
            }
            if matches!(query, Query::TaskView { .. } | Query::TaskOverviews) {
                let mut observation = self.observe(query.clone(), None, maximum_files).await?;
                observation.reset = cursor.is_some();
                return super::recent::observe(observation, &query, maximum_files, maximum_bytes);
            }
            let mut observation = self
                .observe(query.clone(), cursor.clone(), maximum_files)
                .await?;
            let (mut sizes, mut bytes) = (BTreeMap::new(), 0);
            if let Some(snapshot) = &observation.snapshot {
                for file in &snapshot.files {
                    include(&mut sizes, &mut bytes, file, maximum_files)?;
                }
            } else {
                (sizes, bytes) = self
                    .lengths_at(&query, &cursor.ok_or(Error::ResetRequired)?, maximum_files)
                    .await?;
            }
            if bytes > maximum_bytes {
                return Err(Error::LimitExceeded);
            }
            observation.changes = Box::pin(futures::stream::try_unfold(
                (observation.changes, sizes, bytes),
                move |(mut changes, mut sizes, mut bytes)| async move {
                    let Some(change) = changes.next().await else {
                        return Ok(None);
                    };
                    let change = change?;
                    track(&mut sizes, &mut bytes, &change, maximum_files)?;
                    if bytes > maximum_bytes {
                        return Err(Error::LimitExceeded);
                    }
                    Ok(Some((change, (changes, sizes, bytes))))
                },
            ));
            Ok(observation)
        })
    }
}

fn include(
    sizes: &mut BTreeMap<FileKey, usize>,
    bytes: &mut usize,
    file: &FileValue,
    maximum_files: usize,
) -> Result<()> {
    let old = sizes.get(&file.key).copied().unwrap_or(0);
    if !sizes.contains_key(&file.key) && sizes.len() == maximum_files {
        return Err(Error::LimitExceeded);
    }
    *bytes = bytes
        .checked_sub(old)
        .and_then(|n| n.checked_add(file.bytes.len()))
        .ok_or(Error::LimitExceeded)?;
    sizes.insert(file.key.clone(), file.bytes.len());
    Ok(())
}

fn track(
    sizes: &mut BTreeMap<FileKey, usize>,
    bytes: &mut usize,
    change: &Change,
    maximum_files: usize,
) -> Result<()> {
    for key in &change.removed {
        if let Some(length) = sizes.remove(key) {
            *bytes = bytes.checked_sub(length).ok_or(Error::InvalidSegment)?;
        }
    }
    for file in change.file.iter().chain(&change.files) {
        include(sizes, bytes, file, maximum_files)?;
    }
    Ok(())
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    async fn lengths_at(
        &self,
        query: &Query,
        cursor: &Cursor,
        maximum_files: usize,
    ) -> Result<(BTreeMap<FileKey, usize>, usize)> {
        let (mut sizes, mut bytes) = (BTreeMap::new(), 0);
        {
            let index = self.index.lock().await;
            let generation = self.generation(&index);
            if generation.position == cursor.position && generation.commit == cursor.commit {
                index.visit::<true>(query, |stored| {
                    include(&mut sizes, &mut bytes, &stored.file, maximum_files)
                })?;
                return Ok((sizes, bytes));
            }
        }
        // An older cut needs only selection lengths, never retained payload copies.
        // Replay the existing checked feed to that exact commit, then discard it.
        let mut records = self
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from: 0,
                limit: 1,
            })
            .await?;
        let header = records.next().await.ok_or(Error::ResetRequired)??;
        if header.sequence != 0 {
            return Err(Error::ResetRequired);
        }
        let mut start = cursor.clone();
        start.position = 1;
        start.commit = *header.commit_id.as_bytes();
        start.durable = start.durable.min(1);
        if start.position == cursor.position {
            return if start.commit == cursor.commit {
                Ok((sizes, bytes))
            } else {
                Err(Error::ResetRequired)
            };
        }
        let mut changes = self.follow(query.clone(), start).await?;
        while let Some(change) = changes.next().await {
            let change = change?;
            track(&mut sizes, &mut bytes, &change, maximum_files)?;
            if change.cursor.position == cursor.position {
                return if change.cursor.commit == cursor.commit {
                    Ok((sizes, bytes))
                } else {
                    Err(Error::ResetRequired)
                };
            }
        }
        Err(Error::ResetRequired)
    }
}
