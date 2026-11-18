use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// Retained outside the replaceable replay cache, dispatched after durability.
#[derive(Default)]
pub(super) struct Changes {
    keys: BTreeSet<StreamPath>,
    records: BTreeMap<StreamPath, (Vec<Record>, u64)>,
    created: BTreeSet<StreamPath>,
}

// Declared before the cache guard, so early return/cancellation drops that guard
// before dispatching notifications. No user waker runs under a cache lock.
pub(super) struct Dispatch {
    provider: BrowserStream,
    pub(super) keys: Changes,
    pub(super) repair: bool,
    pub(super) failed: Option<bool>,
}
impl Dispatch {
    pub(super) fn new(provider: &BrowserStream) -> Self {
        Self {
            provider: provider.clone(),
            keys: Changes::default(),
            repair: false,
            failed: None,
        }
    }
}
impl Drop for Dispatch {
    fn drop(&mut self) {
        if let Some(unavailable) = self.failed {
            if unavailable {
                self.provider.inner.revisions.close();
            }
            let _ = self.provider.inner.publications.interests.notify_all();
            self.provider.inner.publications.invalidate_follows();
        } else {
            for key in &self.keys.keys {
                let _ = self.provider.inner.publications.interests.notify(key);
            }
            let created = self.keys.created.iter().map(|path| (path, None));
            let published = self
                .keys
                .records
                .iter()
                .map(|(path, batch)| (path, Some(batch)));
            for (path, batch) in created.chain(published) {
                let index = self
                    .provider
                    .inner
                    .publications
                    .follows
                    .get(path)
                    .ok()
                    .flatten();
                if let Some(index) = index {
                    match batch {
                        None => crate::follow_index::FollowIndex::created(&index),
                        Some((records, tail)) => {
                            crate::follow_index::FollowIndex::publish(&index, records, *tail);
                        }
                    }
                }
            }
        }
        if self.repair {
            let _ = self.provider.inner.publications.wake(false);
        }
    }
}

pub(super) struct Before(Vec<(StreamPath, Option<u64>)>);
impl Before {
    pub(super) async fn capture(
        provider: &MemoryStream,
        command: &WriteRequest,
        publications: &notifications::Publications,
    ) -> Result<Self, StreamError> {
        let mut paths = BTreeSet::new();
        let mut capture = |path: &StreamPath, fork| -> Result<(), StreamError> {
            // A fork can create a followed descendant without appending to it.
            // Consult active subscriptions, never enumerate the forked subtree.
            if fork {
                let descendants = path.descendants();
                paths.extend(publications.interests.keys_in(descendants.bounds())?);
                paths.extend(publications.follows.keys_in(descendants.bounds())?);
            }
            let mut next = Some(path.clone());
            while let Some(path) = next {
                next = path.parent();
                paths.insert(path);
            }
            Ok(())
        };
        match command {
            WriteRequest::Append(request) => capture(&request.path, false)?,
            WriteRequest::Fork(request) => capture(&request.destination, true)?,
            WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => {
                for mutation in request.mutations() {
                    capture(
                        mutation.path(),
                        matches!(mutation, CommitMutation::Fork { .. }),
                    )?;
                }
            }
        }
        // The input set has already deduplicated and ordered these paths.
        let mut before = Vec::with_capacity(paths.len());
        for path in paths {
            let tail = provider.publication_tail(&path).await?;
            before.push((path, tail));
        }
        Ok(Self(before))
    }
    pub(super) async fn collect(
        self,
        provider: &MemoryStream,
        keys: &mut Changes,
        publications: &notifications::Publications,
    ) -> Result<(), StreamError> {
        for (path, before) in self.0 {
            let after = provider.publication_tail(&path).await?;
            if after == before {
                continue;
            }
            keys.keys.insert(path.clone());
            let followed = publications.follows.get(&path)?.is_some();
            let Some(after) = after.filter(|_| followed) else {
                continue;
            };
            if before.is_none() {
                keys.created.insert(path.clone());
            }
            let records = keys.records.entry(path.clone()).or_default();
            records.1 = after;
            let mut next = before.unwrap_or(0);
            while next < after {
                let mut batch = provider
                    .read(ReadRequest {
                        key: None,
                        path: path.clone(),
                        from: next,
                        limit: acyclic_stream_wire::wire::StreamLimit::MaxItems as u32,
                        at_least: Default::default(),
                    })
                    .await?;
                let initial = next;
                while let Some(record) = batch.next().await {
                    let record = record?;
                    next = record
                        .sequence
                        .checked_add(1)
                        .ok_or(StreamError::OutOfRange)?;
                    records.0.push(record);
                }
                if next <= initial {
                    return Err(StreamError::Unavailable);
                }
            }
        }
        Ok(())
    }
}

impl BrowserStream {
    pub(super) async fn refresh_publication(&self, force: bool) -> Result<(), StreamError> {
        let mut dispatch = Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        if !force && cache.valid {
            return Ok(());
        }
        #[cfg(test)]
        self.inner.refreshes.set(self.inner.refreshes.get() + 1);
        self.refresh_observation(&mut cache, &mut dispatch).await?;
        Ok(())
    }
    pub(super) async fn with_cached_provider<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut dispatch = Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        if !cache.valid {
            self.refresh_observation(&mut cache, &mut dispatch).await?;
        }
        #[cfg(test)]
        self.inner
            .follow_reads
            .set(self.inner.follow_reads.get() + 1);
        operation(&cache.provider).await
    }
}

pub(super) struct Observer<'a> {
    pub(super) stream: &'a BrowserStream,
    pub(super) changed: &'a mut Changes,
    pub(super) announced: u64,
    pub(super) before: Option<Before>,
}
impl crate::replicated::ReplayObserver for Observer<'_> {
    async fn observe<'a>(
        &'a mut self,
        provider: &'a MemoryStream,
        command: &'a WriteRequest,
        cursor: u64,
        after: bool,
    ) -> Result<(), StreamError> {
        if after {
            if let Some(before) = self.before.take() {
                before
                    .collect(provider, self.changed, &self.stream.inner.publications)
                    .await?;
                self.announced = cursor;
            }
        } else {
            self.before = if cursor >= self.announced {
                Some(Before::capture(provider, command, &self.stream.inner.publications).await?)
            } else {
                None
            };
        }
        Ok(())
    }
}
